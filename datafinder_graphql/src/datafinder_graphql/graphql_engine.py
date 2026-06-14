import datetime
import json
import urllib.request

import numpy as np
import pandas as pd

from datafinder import Attribute, Operation, DataFrame, QueryRunnerBase
from model.graphql_mapping import (
    GraphQLQuery,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)
from model.relational import (
    AggregateOperation,
    AggregateOperator,
    ColumnWithJoin,
    ComparisonOperation,
    ComparisonOperator,
    DateArithmeticOperation,
    DateDiffOperation,
    DateExtractOperation,
    DatePart,
    IsNotNullOperation,
    IsNullOperation,
    LogicalOperation,
    LogicalOperator,
    NoOperation,
    SortDirection,
    SortOperation,
    WindowFunctionOperation,
)


class GraphQLOutput(DataFrame):

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_numpy(self) -> np.ndarray:
        if self._df.empty:
            return np.array([], dtype='object')
        return self._df.values

    def to_pandas(self) -> pd.DataFrame:
        return self._df.copy()


# ---------------------------------------------------------------------------
# Column metadata helpers
# ---------------------------------------------------------------------------

def _col_field_name(col) -> str | None:
    """Return the underlying GraphQL field name for a column expression."""
    if isinstance(col, Attribute):
        return col.column().name
    if isinstance(col, (AggregateOperation, WindowFunctionOperation,
                        DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
        if col.element and isinstance(col.element, ColumnWithJoin):
            return col.element.column.name
    return None


def _col_display_name(col) -> str:
    if isinstance(col, Attribute):
        return col.display_name()
    if isinstance(col, (AggregateOperation, WindowFunctionOperation,
                        DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
        return col.display_name or ""
    return ""


# ---------------------------------------------------------------------------
# Derived column computation
# ---------------------------------------------------------------------------

_DATE_EXTRACT = {
    DatePart.YEAR:    lambda s: s.dt.year,
    DatePart.MONTH:   lambda s: s.dt.month,
    DatePart.DAY:     lambda s: s.dt.day,
    DatePart.HOUR:    lambda s: s.dt.hour,
    DatePart.MINUTE:  lambda s: s.dt.minute,
    DatePart.SECOND:  lambda s: s.dt.second,
    DatePart.QUARTER: lambda s: s.dt.quarter,
    DatePart.WEEK:    lambda s: s.dt.isocalendar().week.astype(int),
    DatePart.DOW:     lambda s: ((s.dt.dayofweek + 1) % 7),  # 0=Sun (DuckDB convention)
}

_ARITH_TIMEDELTA = {
    DatePart.DAY:    lambda n: pd.to_timedelta(n, unit='D'),
    DatePart.HOUR:   lambda n: pd.to_timedelta(n, unit='h'),
    DatePart.MINUTE: lambda n: pd.to_timedelta(n, unit='min'),
    DatePart.SECOND: lambda n: pd.to_timedelta(n, unit='s'),
}


def _compute_derived(col, df: pd.DataFrame) -> pd.Series | None:
    """Apply a date extract / arithmetic / diff column operation to the DataFrame."""
    if isinstance(col, DateExtractOperation):
        fn = _col_field_name(col)
        if not fn or fn not in df.columns:
            return None
        dt = pd.to_datetime(df[fn])
        extractor = _DATE_EXTRACT.get(col.part)
        return extractor(dt) if extractor else None

    if isinstance(col, DateArithmeticOperation):
        fn = _col_field_name(col)
        if not fn or fn not in df.columns:
            return None
        dt = pd.to_datetime(df[fn])
        n = col.n if col.is_add else -col.n
        if col.unit in _ARITH_TIMEDELTA:
            result = dt + _ARITH_TIMEDELTA[col.unit](n)
        elif col.unit == DatePart.MONTH:
            result = dt.apply(lambda x: x + pd.DateOffset(months=n))
        elif col.unit == DatePart.YEAR:
            result = dt.apply(lambda x: x + pd.DateOffset(years=n))
        else:
            return None
        return result.astype('datetime64[us]')

    if isinstance(col, DateDiffOperation):
        fn = _col_field_name(col)
        if not fn or fn not in df.columns:
            return None
        dt = pd.to_datetime(df[fn])
        other = pd.Timestamp(col.other)
        delta = other - dt
        if col.unit == DatePart.DAY:
            return delta.dt.days
        if col.unit == DatePart.HOUR:
            return (delta.dt.total_seconds() / 3600).astype(int)
        if col.unit == DatePart.MINUTE:
            return (delta.dt.total_seconds() / 60).astype(int)
        if col.unit == DatePart.SECOND:
            return delta.dt.total_seconds().astype(int)
        if col.unit == DatePart.MONTH:
            return dt.apply(lambda x: (other.year - x.year) * 12 + (other.month - x.month))
        if col.unit == DatePart.YEAR:
            return dt.apply(lambda x: other.year - x.year)
        return None

    return None


def _project_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Project and rename columns, computing derived expressions as needed."""
    result: dict = {}
    for col in columns:
        dn = _col_display_name(col)
        derived = _compute_derived(col, df)
        if derived is not None:
            result[dn] = derived.values
        elif isinstance(col, Attribute):
            fn = col.column().name
            if fn in df.columns:
                result[dn] = df[fn].values
    return pd.DataFrame(result)


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def _op_field_names(op) -> set[str]:
    if op is None or isinstance(op, NoOperation):
        return set()
    if isinstance(op, ComparisonOperation):
        if isinstance(op.left, ColumnWithJoin):
            return {op.left.column.name}
        return set()
    if isinstance(op, LogicalOperation):
        return _op_field_names(op.left) | _op_field_names(op.right)
    if isinstance(op, (IsNullOperation, IsNotNullOperation)):
        if isinstance(op.element, ColumnWithJoin):
            return {op.element.column.name}
        return set()
    return set()


def _constant_value(op):
    return getattr(op, 'value', op)


_DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")


def _parse_dt_str(s) -> datetime.datetime | None:
    """Parse a datetime string using Python's datetime (handles 9999-12-31 sentinel)."""
    if s is None:
        return None
    text = str(s).strip()
    for fmt in _DT_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def _eval_filter(df: pd.DataFrame, op) -> pd.Series:
    if op is None or isinstance(op, NoOperation):
        return pd.Series([True] * len(df), index=df.index)

    if isinstance(op, ComparisonOperation):
        if isinstance(op.left, ColumnWithJoin):
            name = op.left.column.name
            if name not in df.columns:
                return pd.Series([True] * len(df), index=df.index)
            col = df[name]
            val = _constant_value(op.right)
            # Parse string columns when comparing to datetime/date values.
            # Use element-wise Python datetime parsing to handle out-of-bounds
            # sentinel values like "9999-12-31 23:59:59".
            if isinstance(val, (datetime.datetime, datetime.date)) and col.dtype == object:
                if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                    val = datetime.datetime(val.year, val.month, val.day)
                col = col.apply(_parse_dt_str)
            if op.operator == ComparisonOperator.EQUAL:
                return col == val
            if op.operator == ComparisonOperator.NOT_EQUAL:
                return col != val
            if op.operator == ComparisonOperator.LESS_THAN:
                return col < val
            if op.operator == ComparisonOperator.GREATER_THAN:
                return col > val
            if op.operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO:
                return col <= val
            if op.operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO:
                return col >= val
        return pd.Series([True] * len(df), index=df.index)

    if isinstance(op, LogicalOperation):
        left_mask = _eval_filter(df, op.left)
        right_mask = _eval_filter(df, op.right)
        if op.operator == LogicalOperator.AND:
            return left_mask & right_mask
        return left_mask | right_mask

    if isinstance(op, (IsNullOperation, IsNotNullOperation)):
        if isinstance(op.element, ColumnWithJoin):
            name = op.element.column.name
            if name in df.columns:
                return df[name].isna() if isinstance(op, IsNullOperation) else df[name].notna()
    return pd.Series([True] * len(df), index=df.index)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

_AGG_FUNCS = {
    AggregateOperator.SUM: 'sum',
    AggregateOperator.MIN: 'min',
    AggregateOperator.MAX: 'max',
    AggregateOperator.COUNT: 'count',
    AggregateOperator.AVERAGE: 'mean',
}


def _apply_aggregation(df: pd.DataFrame, columns: list, group_by: list | None) -> pd.DataFrame:
    agg_specs: list[tuple[str, str, str]] = []
    gb_fields: list[str] = []

    for col in columns:
        if isinstance(col, AggregateOperation):
            fn = _col_field_name(col)
            if fn:
                agg_specs.append((fn, _AGG_FUNCS.get(col.operator, 'sum'), _col_display_name(col)))

    if group_by:
        for gb in group_by:
            if isinstance(gb, Attribute):
                gb_fields.append(gb.column().name)

    if gb_fields:
        agg_dict = {fn: func for fn, func, _ in agg_specs}
        result = df.groupby(gb_fields).agg(agg_dict).reset_index()
        result = result.rename(columns={fn: dn for fn, _, dn in agg_specs})
    else:
        rows: dict = {}
        for fn, func, dn in agg_specs:
            if fn in df.columns:
                rows[dn] = [getattr(df[fn], func)()]
        result = pd.DataFrame(rows)

    return result


# ---------------------------------------------------------------------------
# GraphQL request helpers
# ---------------------------------------------------------------------------

def _flatten_row(row: dict, prefix: str = "") -> dict:
    result = {}
    for k, v in row.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result.update(_flatten_row(v, key))
        else:
            result[key] = v
    return result


def _build_fields_str(columns: list, extra_fields: set[str]) -> str:
    all_names: list[str] = []
    seen: set[str] = set()
    for col in columns:
        fn = _col_field_name(col)
        if fn and fn not in seen:
            all_names.append(fn)
            seen.add(fn)
    for fn in extra_fields:
        if fn not in seen:
            all_names.append(fn)
            seen.add(fn)

    nested: dict[str, list[str]] = {}
    flat: list[str] = []
    for name in all_names:
        if "." in name:
            parent, child = name.split(".", 1)
            nested.setdefault(parent, []).append(child)
        else:
            flat.append(name)
    parts = list(flat)
    for parent, children in nested.items():
        parts.append(f"{parent} {{ {' '.join(children)} }}")
    return " ".join(parts)


def _build_temporal_args(business_date: datetime.date | None,
                         processing_datetime: datetime.datetime | None,
                         milestone) -> list[str]:
    if milestone is None:
        return []
    args = []
    if isinstance(milestone, GraphQLBiTemporalMilestone):
        if business_date is not None:
            args.append(f'{milestone.business_date_argument}: "{business_date}"')
        if processing_datetime is not None:
            args.append(f'{milestone.processing_argument}: "{processing_datetime.isoformat()}"')
    elif isinstance(milestone, GraphQLBusinessDateMilestone):
        if business_date is not None:
            args.append(f'{milestone.argument_name}: "{business_date}"')
    elif isinstance(milestone, GraphQLProcessingMilestone):
        if processing_datetime is not None:
            args.append(f'{milestone.argument_name}: "{processing_datetime.isoformat()}"')
    return args


def _fetch_rows(table: GraphQLQuery, fields_str: str,
                business_date: datetime.date | None,
                processing_datetime: datetime.datetime | None,
                timeout_ms: int) -> list[dict]:
    milestone = getattr(table, 'milestone', None)
    arg_parts = _build_temporal_args(business_date, processing_datetime, milestone)
    args = f"({', '.join(arg_parts)})" if arg_parts else ""
    query_str = f"{{ {table.name}{args} {{ {fields_str} }} }}"
    payload = json.dumps({"query": query_str}).encode()
    req = urllib.request.Request(
        table.endpoint.url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_ms / 1000) as response:
            result = json.loads(response.read())
    except TimeoutError:
        raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
    return result["data"][table.name]


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class GraphQLConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime,  # type: ignore[override]
               columns: list, table: GraphQLQuery, op: Operation,
               order_by: list | None = None, group_by: list | None = None, limit: int | None = None,
               timeout_ms: int = 60_000, business_date_to: datetime.date | None = None) -> DataFrame:

        extra_fields = _op_field_names(op)
        fields_str = _build_fields_str(columns, extra_fields)
        has_agg = any(isinstance(col, AggregateOperation) for col in columns)
        display_names = [_col_display_name(col) for col in columns]

        # Fetch rows — iterate dates for date-range queries
        if business_date_to is not None and business_date is not None:
            all_rows: list[dict] = []
            current = business_date
            while current <= business_date_to:
                all_rows.extend(_fetch_rows(table, fields_str, current, processing_datetime, timeout_ms))
                current += datetime.timedelta(days=1)
            rows = all_rows
        else:
            rows = _fetch_rows(table, fields_str, business_date, processing_datetime, timeout_ms)

        if not rows:
            return GraphQLOutput(pd.DataFrame(columns=display_names))

        df = pd.DataFrame([_flatten_row(row) for row in rows])

        # Apply filter
        if op is not None and not isinstance(op, NoOperation):
            mask = _eval_filter(df, op)
            df = df[mask].reset_index(drop=True)

        if df.empty:
            return GraphQLOutput(pd.DataFrame(columns=display_names))

        # Aggregation path
        if has_agg:
            return GraphQLOutput(_apply_aggregation(df, columns, group_by))

        # Sort before projecting (field names still raw)
        if order_by:
            sort_cols, ascending = [], []
            for so in order_by:
                if isinstance(so, SortOperation) and isinstance(so.column, ColumnWithJoin):
                    fn = so.column.column.name
                    if fn in df.columns:
                        sort_cols.append(fn)
                        ascending.append(so.direction == SortDirection.ASC)
            if sort_cols:
                df = df.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)

        # Project and apply derived column expressions
        result_df = _project_columns(df, columns)

        if limit is not None:
            result_df = result_df.head(limit)

        return GraphQLOutput(result_df)
