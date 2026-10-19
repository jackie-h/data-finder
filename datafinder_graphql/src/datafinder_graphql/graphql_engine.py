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
    ConstantOperation,
    DateArithmeticOperation,
    DateDiffOperation,
    DateExtractOperation,
    DatePart,
    IsNotNullOperation,
    IsNullOperation,
    LogicalOperation,
    LogicalOperator,
    NoOperation,
    RelationalOperationElement,
    ScalarFunction,
    ScalarFunctionOperation,
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

class GraphQLPushdownError(Exception):
    """A filter, sort or limit that cannot be expressed as arguments to the GraphQL server.

    The engine does not compensate on the client: only aggregations are computed locally.
    """


def _col_field_name(col) -> str | None:
    """Return the underlying GraphQL field name for a column expression."""
    if isinstance(col, Attribute):
        return col.column().name
    if isinstance(col, (AggregateOperation, WindowFunctionOperation, ScalarFunctionOperation,
                        DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
        if col.element and isinstance(col.element, ColumnWithJoin):
            return col.element.column.name
    return None


def _col_display_name(col) -> str:
    if isinstance(col, Attribute):
        return col.display_name()
    if isinstance(col, (AggregateOperation, WindowFunctionOperation, ScalarFunctionOperation,
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


def _round_half_away_from_zero(s: pd.Series, decimals: int) -> pd.Series:
    """Round like SQL (halves go away from zero), not like pandas (halves go to even)."""
    scale = 10 ** decimals
    return np.sign(s) * np.floor(s.abs() * scale + 0.5) / scale


_NUMERIC_SCALAR = {
    ScalarFunction.ABS:     lambda s, n: s.abs(),
    ScalarFunction.CEILING: lambda s, n: np.ceil(s),
    ScalarFunction.FLOOR:   lambda s, n: np.floor(s),
    ScalarFunction.SQRT:    lambda s, n: np.sqrt(s),
    ScalarFunction.MOD:     lambda s, n: np.fmod(s, n),
    ScalarFunction.POWER:   lambda s, n: np.power(s, n),
    ScalarFunction.ROUND:   lambda s, n: _round_half_away_from_zero(s, n or 0),
}


def _compute_derived(col, df: pd.DataFrame) -> pd.Series | None:
    """Apply a numeric function / date extract / arithmetic / diff column operation to the DataFrame."""
    if isinstance(col, ScalarFunctionOperation):
        fn = _col_field_name(col)
        func = _NUMERIC_SCALAR.get(col.function)
        if not fn or fn not in df.columns or func is None:
            return None
        return pd.Series(func(pd.to_numeric(df[fn]), col.second_arg))

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
            result = pd.Series(dt.apply(lambda x: x + pd.DateOffset(months=n)))
        elif col.unit == DatePart.YEAR:
            result = pd.Series(dt.apply(lambda x: x + pd.DateOffset(years=n)))
        else:
            return None
        return pd.Series(result).astype('datetime64[us]')

    if isinstance(col, DateDiffOperation):
        fn = _col_field_name(col)
        if not fn or fn not in df.columns:
            return None
        dt = pd.to_datetime(df[fn])
        other = pd.Timestamp(col.other)
        delta = other - dt
        if col.unit == DatePart.DAY:
            return pd.Series(delta.dt.days)
        if col.unit == DatePart.HOUR:
            return pd.Series((delta.dt.total_seconds() / 3600).astype(int))
        if col.unit == DatePart.MINUTE:
            return pd.Series((delta.dt.total_seconds() / 60).astype(int))
        if col.unit == DatePart.SECOND:
            return pd.Series(delta.dt.total_seconds().astype(int))
        if col.unit == DatePart.MONTH:
            return pd.Series(dt.apply(lambda x: (other.year - x.year) * 12 + (other.month - x.month)))
        if col.unit == DatePart.YEAR:
            return pd.Series(dt.apply(lambda x: other.year - x.year))
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
            # a nested field is absent from every row when its parent object is null in all of them
            result[dn] = df[fn].values if fn in df.columns else [None] * len(df)
    return pd.DataFrame(result)


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def _constant_value(op):
    return getattr(op, 'value', op)


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
        result = result.rename(columns={
            gb.column().name: gb.display_name() for gb in group_by or [] if isinstance(gb, Attribute)})
    else:
        rows: dict = {}
        for fn, func, dn in agg_specs:
            if fn in df.columns:
                rows[dn] = [getattr(df[fn], func)()]
        result = pd.DataFrame(rows)

    return result


def _sort_aggregated(df: pd.DataFrame, order_by: list, group_by: list | None) -> pd.DataFrame:
    """Sort an aggregation's result, which only exists on the client. It can be sorted by its group-by columns."""
    display_by_field = {gb.column().name: gb.display_name() for gb in group_by or [] if isinstance(gb, Attribute)}
    sort_cols: list[str] = []
    ascending: list[bool] = []
    for so in order_by:
        field = _field_name(so.column, "a sort") if isinstance(so, SortOperation) else None
        if field not in display_by_field:
            raise GraphQLPushdownError("An aggregation can only be sorted by its group-by columns")
        sort_cols.append(display_by_field[field])
        ascending.append(so.direction == SortDirection.ASC)
    return pd.DataFrame(df.sort_values(sort_cols, ascending=ascending)).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Hasura server-side push: filter / sort / limit → GraphQL args
# ---------------------------------------------------------------------------

_CMP_OPS = {
    ComparisonOperator.EQUAL:                "_eq",
    ComparisonOperator.NOT_EQUAL:            "_neq",
    ComparisonOperator.LESS_THAN:            "_lt",
    ComparisonOperator.GREATER_THAN:         "_gt",
    ComparisonOperator.LESS_THAN_OR_EQUAL_TO:    "_lte",
    ComparisonOperator.GREATER_THAN_OR_EQUAL_TO: "_gte",
    ComparisonOperator.LIKE:                 "_like",
    ComparisonOperator.NOT_LIKE:             "_nlike",
}


def _gql_value(val) -> str:
    """Serialize a Python value to an inline GraphQL argument literal."""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, datetime.datetime):
        return f'"{val.isoformat()}"'
    if isinstance(val, datetime.date):
        return f'"{val.isoformat()}"'
    if isinstance(val, str):
        return f'"{val}"'
    return str(val)


def _nest(field: str, inner: str) -> str:
    """Wrap inner in the object path for a nested field: "a.b" gives "{ a: { b: inner } }"."""
    out = inner
    for part in reversed(field.split(".")):
        out = f"{{ {part}: {out} }}"
    return out


def _field_name(element, what: str) -> str:
    if not isinstance(element, ColumnWithJoin):
        raise GraphQLPushdownError(
            f"Cannot push {what} to the server: {type(element).__name__} is not a plain field")
    return element.column.name


def _op_to_where_clause(op) -> str | None:
    """Translate a filter op tree to a GraphQL where-object string. Raises if any part cannot be expressed."""
    if op is None or isinstance(op, NoOperation):
        return None
    if isinstance(op, ComparisonOperation):
        gql_op = _CMP_OPS.get(op.operator)
        if gql_op is None:
            raise GraphQLPushdownError(f"Cannot push comparison {op.operator.name} to the server")
        field = _field_name(op.left, "a filter")
        if isinstance(op.right, RelationalOperationElement) and not isinstance(op.right, ConstantOperation):
            raise GraphQLPushdownError("Cannot push a filter comparing two fields to the server")
        return _nest(field, f"{{ {gql_op}: {_gql_value(_constant_value(op.right))} }}")
    if isinstance(op, LogicalOperation):
        left = _op_to_where_clause(op.left)
        right = _op_to_where_clause(op.right)
        logical = "_and" if op.operator == LogicalOperator.AND else "_or"
        return f"{{ {logical}: [{left}, {right}] }}"
    if isinstance(op, IsNullOperation):
        return _nest(_field_name(op.element, "a null check"), "{ _is_null: true }")
    if isinstance(op, IsNotNullOperation):
        return _nest(_field_name(op.element, "a null check"), "{ _is_null: false }")
    raise GraphQLPushdownError(f"Cannot push {type(op).__name__} to the server")


def _to_order_by_clause(order_by: list | None) -> str | None:
    """Translate order_by list to a GraphQL order-by array string. Raises if any part cannot be expressed."""
    if not order_by:
        return None
    parts = []
    for so in order_by:
        if not isinstance(so, SortOperation):
            raise GraphQLPushdownError(f"Cannot push sort by {type(so).__name__} to the server")
        direction = "asc" if so.direction == SortDirection.ASC else "desc"
        parts.append(_nest(_field_name(so.column, "a sort"), direction))
    return f"[{', '.join(parts)}]"


def _build_server_args(op, order_by: list | None, limit: int | None, filter_conv) -> list[str]:
    """Return the GraphQL query args for the filter, sort and limit, using the endpoint's filter convention."""
    where = _op_to_where_clause(op)
    order_by_str = _to_order_by_clause(order_by)
    if where is None and order_by_str is None and limit is None:
        return []
    if filter_conv is None:
        raise GraphQLPushdownError(
            "The endpoint declares no filter convention, so filters, sorts and limits cannot be pushed to the server")
    extra: list[str] = []
    if where:
        extra.append(f"{filter_conv.filter_arg}: {where}")
    if order_by_str:
        extra.append(f"{filter_conv.sort_arg}: {order_by_str}")
    if limit is not None:
        extra.append(f"{filter_conv.limit_arg}: {limit}")
    return extra


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


def _build_fields_str(columns: list) -> str:
    all_names: list[str] = []
    seen: set[str] = set()
    for col in columns:
        fn = _col_field_name(col)
        if fn and fn not in seen:
            all_names.append(fn)
            seen.add(fn)

    # "account.branch.city" selects city inside branch inside account
    tree: dict = {}
    for name in all_names:
        node = tree
        for part in name.split("."):
            node = node.setdefault(part, {})

    def render(node: dict) -> str:
        return " ".join(f"{k} {{ {render(v)} }}" if v else k for k, v in node.items())

    return render(tree)


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
                timeout_ms: int,
                server_args: list[str] | None = None) -> list[dict]:
    milestone = getattr(table, 'milestone', None)
    arg_parts = _build_temporal_args(business_date, processing_datetime, milestone)
    if server_args:
        arg_parts.extend(server_args)
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
    if result.get("errors"):
        raise RuntimeError(f"GraphQL server returned errors: {result['errors']}")
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

        fields_str = _build_fields_str(columns)
        has_agg = any(isinstance(col, AggregateOperation) for col in columns)
        display_names = [_col_display_name(col) for col in columns]

        filter_conv = getattr(table.endpoint, 'filter_convention', None)
        is_date_range = business_date_to is not None and business_date is not None
        if is_date_range and (order_by or limit is not None):
            raise GraphQLPushdownError(
                "A sort or limit cannot be pushed to the server for a date range: each date is a separate query")

        # Everything is pushed to the server; the sort and limit of an aggregation apply to its result, not its input
        server_args = _build_server_args(
            op, None if has_agg else order_by, None if has_agg else limit, filter_conv)

        # Fetch rows — iterate dates for date-range queries
        if business_date_to is not None and business_date is not None:
            all_rows: list[dict] = []
            current = business_date
            while current <= business_date_to:
                all_rows.extend(_fetch_rows(table, fields_str, current, processing_datetime, timeout_ms, server_args))
                current += datetime.timedelta(days=1)
            rows = all_rows
        else:
            rows = _fetch_rows(table, fields_str, business_date, processing_datetime, timeout_ms, server_args)

        if not rows:
            return GraphQLOutput(pd.DataFrame({name: [] for name in display_names}))

        df = pd.DataFrame([_flatten_row(row) for row in rows])

        # Aggregation is the only thing computed on the client
        if has_agg:
            aggregated = _apply_aggregation(df, columns, group_by)
            if order_by:
                aggregated = _sort_aggregated(aggregated, order_by, group_by)
            return GraphQLOutput(aggregated.head(limit) if limit is not None else aggregated)

        return GraphQLOutput(_project_columns(df, columns))
