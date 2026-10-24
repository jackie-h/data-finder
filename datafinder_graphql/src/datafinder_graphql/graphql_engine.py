import datetime
import json
from dataclasses import dataclass, field
import urllib.request

import numpy as np
import pandas as pd

from datafinder import Attribute, Operation, DataFrame, QueryRunnerBase
from model.graphql_mapping import (
    parse_field_path,
    plain_field_path,
    GraphQLQuery,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)
from model.relational import (
    AggregateOperation,
    AggregateOperator,
    Alias,
    ColumnWithJoin,
    ComparisonOperation,
    ComparisonOperator,
    ConstantOperation,
    DateArithmeticOperation,
    DateDiffOperation,
    DateExtractOperation,
    DatePart,
    IsNotNullOperation,
    InOperation,
    NotInOperation,
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

class GraphQLUnsupportedError(Exception):
    """A query the GraphQL engine cannot answer.

    The engine does not compensate on the client for what the server cannot do: it computes aggregations
    and row-wise column expressions locally, and nothing else.
    """


class GraphQLPushdownError(GraphQLUnsupportedError):
    """A filter, sort or limit that cannot be expressed as arguments to the GraphQL server."""


def _plain(name: str) -> str:
    """A field path without its to-many markers and filters: "manager.projects[].name" is "manager.projects.name"."""
    return plain_field_path(name)


def _col_raw_name(col) -> str | None:
    """The field path of a column expression, with to-many markers."""
    if isinstance(col, Attribute):
        return col.column().name
    if isinstance(col, (AggregateOperation, WindowFunctionOperation, ScalarFunctionOperation,
                        DateExtractOperation, DateArithmeticOperation, DateDiffOperation)):
        if col.element and isinstance(col.element, ColumnWithJoin):
            return col.element.column.name
    return None


def _col_field_name(col) -> str | None:
    """The underlying GraphQL field path for a column expression."""
    raw = _col_raw_name(col)
    return _plain(raw) if raw is not None else None


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


def _num(s: pd.Series) -> pd.Series:
    return pd.Series(pd.to_numeric(s))


def _right(s: pd.Series, n: int) -> pd.Series:
    return s.map(lambda x: x if x is None else x[max(len(x) - n, 0):])


def _substring(s: pd.Series, args: list) -> pd.Series:
    start = args[0] - 1  # SQL positions are 1-based
    stop = None if len(args) == 1 else start + args[1]
    return s.map(lambda x: x if x is None else x[start:stop])


# Row-wise functions, computed on the returned rows; each takes the column and the operation's arguments
_SCALAR_FUNCS: dict = {
    ScalarFunction.ABS:       lambda s, c: _num(s).abs(),
    ScalarFunction.CEILING:   lambda s, c: np.ceil(_num(s)),
    ScalarFunction.FLOOR:     lambda s, c: np.floor(_num(s)),
    ScalarFunction.SQRT:      lambda s, c: np.sqrt(_num(s)),
    ScalarFunction.MOD:       lambda s, c: np.fmod(_num(s), c.second_arg),
    ScalarFunction.POWER:     lambda s, c: np.power(_num(s), c.second_arg),
    ScalarFunction.ROUND:     lambda s, c: _round_half_away_from_zero(_num(s), c.second_arg or 0),
    ScalarFunction.UPPER:     lambda s, c: s.str.upper(),
    ScalarFunction.LOWER:     lambda s, c: s.str.lower(),
    ScalarFunction.TRIM:      lambda s, c: s.str.strip(),
    ScalarFunction.LTRIM:     lambda s, c: s.str.lstrip(),
    ScalarFunction.RTRIM:     lambda s, c: s.str.rstrip(),
    ScalarFunction.LENGTH:    lambda s, c: s.str.len(),
    ScalarFunction.REVERSE:   lambda s, c: s.map(lambda x: x if x is None else x[::-1]),
    ScalarFunction.LEFT:      lambda s, c: s.str[:c.extra_args[0]],
    ScalarFunction.RIGHT:     lambda s, c: _right(s, c.extra_args[0]),
    ScalarFunction.REPEAT:    lambda s, c: s.str.repeat(c.extra_args[0]),
    ScalarFunction.REPLACE:   lambda s, c: s.str.replace(c.extra_args[0], c.extra_args[1], regex=False),
    ScalarFunction.SUBSTRING: lambda s, c: _substring(s, c.extra_args),
}


def _compute_derived(col, df: pd.DataFrame) -> pd.Series | None:
    """Apply a scalar function / date extract / arithmetic / diff column operation to the DataFrame."""
    if isinstance(col, ScalarFunctionOperation):
        fn = _col_field_name(col)
        func = _SCALAR_FUNCS.get(col.function)
        if func is None:
            raise GraphQLUnsupportedError(f"The GraphQL engine does not support the function {col.function.name}")
        if not fn or fn not in df.columns:
            return None
        return pd.Series(func(df[fn], col))

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


def _project_columns(df: pd.DataFrame, columns: list, names: list[str] | None = None) -> pd.DataFrame:
    """Project and rename columns, computing derived expressions as needed. ``names`` are the result's column names,
    which differ from the display names where a column has been given an alias."""
    result: dict = {}
    for i, col in enumerate(columns):
        dn = names[i] if names else _col_display_name(col)
        derived = _compute_derived(col, df)
        if derived is not None:
            result[dn] = derived.values
        elif isinstance(col, Attribute):
            fn = _plain(col.column().name)
            # a nested field is absent from every row when its parent object is null in all of them
            result[dn] = df[fn].values if fn in df.columns else [None] * len(df)
        else:
            raise GraphQLUnsupportedError(f"The GraphQL engine does not support {type(col).__name__} columns")
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
                gb_fields.append(_plain(gb.column().name))

    if gb_fields:
        agg_dict = {fn: func for fn, func, _ in agg_specs}
        result = df.groupby(gb_fields).agg(agg_dict).reset_index()
        result = result.rename(columns={fn: dn for fn, _, dn in agg_specs})
        result = result.rename(columns={
            _plain(gb.column().name): gb.display_name() for gb in group_by or [] if isinstance(gb, Attribute)})
    else:
        rows: dict = {}
        for fn, func, dn in agg_specs:
            if fn in df.columns:
                rows[dn] = [getattr(df[fn], func)()]
        result = pd.DataFrame(rows)

    return result


def _slice_rows(df: pd.DataFrame, offset: int | None, limit: int | None) -> pd.DataFrame:
    """Skip ``offset`` rows, then keep ``limit`` of the rest."""
    start = offset or 0
    return pd.DataFrame(df.iloc[start:start + limit] if limit is not None else df.iloc[start:]).reset_index(drop=True)


def _sort_aggregated(df: pd.DataFrame, order_by: list, group_by: list | None) -> pd.DataFrame:
    """Sort an aggregation's result, which only exists on the client. It can be sorted by its group-by columns."""
    display_by_field = {_plain(gb.column().name): gb.display_name() for gb in group_by or [] if isinstance(gb, Attribute)}
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
    return _plain(element.column.name)


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
    if isinstance(op, (InOperation, NotInOperation)):
        field = _field_name(op.element, "a filter")
        values = ", ".join(_gql_value(_constant_value(v)) for v in op.values)
        if isinstance(op, InOperation):
            return _nest(field, f"{{ _in: [{values}] }}")
        not_in = _nest(field, f"{{ _nin: [{values}] }}")
        # a server's _nin skips nulls as SQL does, so a nullable field has to match its nulls explicitly
        if op.element.column.nullable:  # type: ignore[union-attr]  # _field_name checked it is a ColumnWithJoin
            return f"{{ _or: [{not_in}, {_nest(field, '{ _is_null: true }')}] }}"
        return not_in
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
        if _is_many(so.column):
            raise GraphQLPushdownError("Cannot push a sort on a to-many relation to the server: select the column too")
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


def _is_many(element) -> bool:
    return isinstance(element, ColumnWithJoin) and any(seg.many for seg in parse_field_path(element.column.name))


def _op_raw_names(op) -> set[str]:
    """The field paths a filter refers to, with to-many markers."""
    if isinstance(op, ComparisonOperation) and isinstance(op.left, ColumnWithJoin):
        return {op.left.column.name}
    if isinstance(op, LogicalOperation):
        return _op_raw_names(op.left) | _op_raw_names(op.right)
    if isinstance(op, (IsNullOperation, IsNotNullOperation, InOperation, NotInOperation)) \
            and isinstance(op.element, ColumnWithJoin):
        return {op.element.column.name}
    return set()


def _relation(raw: str) -> str | None:
    """The plain path to the first to-many relation on a field path, or None."""
    names: list[str] = []
    for seg in parse_field_path(raw):
        names.append(seg.name)
        if seg.many:
            return ".".join(names)
    return None


def _check_expansion(op, columns: list, limit: int | None) -> bool:
    """Whether the rows are expanded by a to-many relation, raising if the server cannot answer the query as is."""
    selected = [raw for raw in (_col_raw_name(col) for col in columns) if raw]
    expands = any(_relation(raw) for raw in selected)
    if expands and limit is not None:
        raise GraphQLUnsupportedError("A limit cannot be pushed to the server for rows expanded by a to-many relation")
    # a server filters the parents on a to-many condition but returns all of their children
    for raw in _op_raw_names(op):
        relation = _relation(raw)
        if relation is None:
            continue
        if any(_plain(sel).startswith(relation + ".") for sel in selected):
            raise GraphQLUnsupportedError(
                f"Cannot filter and select the same to-many relation ({relation}): "
                "the server returns all of a matching parent's children")
    return expands


def _sort_expanded(df: pd.DataFrame, order_by: list) -> pd.DataFrame:
    """Sort rows that a to-many relation expanded: the server can only order the parents."""
    fields: list[str] = []
    ascending: list[bool] = []
    for so in order_by:
        field = _field_name(so.column, "a sort") if isinstance(so, SortOperation) else None
        if field is None or field not in df.columns:
            raise GraphQLUnsupportedError("Rows expanded by a to-many relation can only be sorted by selected columns")
        fields.append(field)
        ascending.append(so.direction == SortDirection.ASC)
    return pd.DataFrame(df.sort_values(fields, ascending=ascending, kind="stable")).reset_index(drop=True)


# ---------------------------------------------------------------------------
# GraphQL request helpers
# ---------------------------------------------------------------------------

def _flatten_rows(row: dict, prefix: str = "") -> list[dict]:
    """Flatten a nested row into dotted keys. A to-many relation expands the row into one row per child, as a
    join does, or into a single row without the relation's fields when it has no children."""
    rows: list[dict] = [{}]
    for k, v in row.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            children = _flatten_rows(v, key)
        elif isinstance(v, list):
            children = [flat for item in v for flat in _flatten_rows(item, key)] or [{}]
        else:
            children = [{key: v}]
        rows = [{**r, **c} for r in rows for c in children]
    return rows


@dataclass
class _FieldNode:
    children: dict = field(default_factory=dict)
    many: bool = False
    filters: dict | None = None


def _filters_to_where(filters: dict) -> str:
    conditions = [f"{{ {name}: {{ _eq: {_gql_value(value)} }} }}" for name, value in filters.items()]
    return conditions[0] if len(conditions) == 1 else f"{{ _and: [{', '.join(conditions)}] }}"


def _build_fields_str(columns: list, filter_conv) -> str:
    """The selection set for the columns. A relation navigated with filters takes them as a where argument."""
    root: dict[str, _FieldNode] = {}
    for col in columns:
        raw = _col_raw_name(col)
        if not raw:
            continue
        children = root
        for seg in parse_field_path(raw):
            node = children.setdefault(seg.name, _FieldNode())
            node.many = seg.many
            if node.filters is None:
                node.filters = seg.filters
            elif node.filters != seg.filters:
                raise GraphQLUnsupportedError(f"The relation {seg.name} is selected with different filters")
            children = node.children

    def render(children: dict) -> str:
        parts = []
        for name, node in children.items():
            args = ""
            if node.filters:
                if not node.many:
                    raise GraphQLPushdownError(
                        f"Cannot filter the to-one relation {name} on the server: only a list can take a where argument")
                if filter_conv is None:
                    raise GraphQLPushdownError(
                        "The endpoint declares no filter convention, so a relation cannot be filtered on the server")
                args = f"({filter_conv.filter_arg}: {_filters_to_where(node.filters)})"
            parts.append(f"{name}{args} {{ {render(node.children)} }}" if node.children else f"{name}{args}")
        return " ".join(parts)

    return render(root)


def _processing_iso(processing: datetime.datetime | datetime.date) -> str:
    """The server takes a timestamp, so a plain processing date is sent as midnight on that date."""
    if not isinstance(processing, datetime.datetime):
        processing = datetime.datetime.combine(processing, datetime.time.min)
    return processing.isoformat()


def _build_temporal_args(business_date: datetime.date | None,
                         processing_datetime: datetime.datetime | datetime.date | None,
                         milestone) -> list[str]:
    if milestone is None:
        return []
    args = []
    if isinstance(milestone, GraphQLBiTemporalMilestone):
        if business_date is not None:
            args.append(f'{milestone.business_date_argument}: "{business_date}"')
        if processing_datetime is not None:
            args.append(f'{milestone.processing_argument}: "{_processing_iso(processing_datetime)}"')
    elif isinstance(milestone, GraphQLBusinessDateMilestone):
        if business_date is not None:
            args.append(f'{milestone.argument_name}: "{business_date}"')
    elif isinstance(milestone, GraphQLProcessingMilestone):
        if processing_datetime is not None:
            args.append(f'{milestone.argument_name}: "{_processing_iso(processing_datetime)}"')
    return args


def _fetch_rows(table: GraphQLQuery, fields_str: str,
                business_date: datetime.date | None,
                processing_datetime: datetime.datetime | datetime.date | None,
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
    def select(business_date: datetime.date, processing_datetime: datetime.datetime | datetime.date,  # type: ignore[override]
               columns: list, table: GraphQLQuery, op: Operation,
               order_by: list | None = None, group_by: list | None = None, limit: int | None = None,
               timeout_ms: int = 60_000, business_date_to: datetime.date | None = None,
               distinct: bool = False, offset: int | None = None) -> DataFrame:

        # an alias only names the column in the result: read the column, then name it
        names = [col.name if isinstance(col, Alias) else _col_display_name(col) for col in columns]
        renames = {_col_display_name(col.element): col.name for col in columns if isinstance(col, Alias)}
        columns = [col.element if isinstance(col, Alias) else col for col in columns]

        filter_conv = getattr(table.endpoint, 'filter_convention', None)
        fields_str = _build_fields_str(columns, filter_conv)
        has_agg = any(isinstance(col, AggregateOperation) for col in columns)

        is_date_range = business_date_to is not None and business_date is not None
        if is_date_range and (order_by or limit is not None):
            raise GraphQLPushdownError(
                "A sort or limit cannot be pushed to the server for a date range: each date is a separate query")

        # Everything is pushed to the server; the sort and limit of an aggregation apply to its result, not its input
        expands = _check_expansion(op, columns, limit)
        # the server can only order the parents, so rows expanded by a to-many relation are sorted afterwards
        sort_expanded = bool(order_by) and not has_agg and expands and \
            any(isinstance(so, SortOperation) and _is_many(so.column) for so in order_by)
        server_args = _build_server_args(
            op, None if has_agg or sort_expanded else order_by, None if has_agg or distinct or offset else limit, filter_conv)

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
            return GraphQLOutput(pd.DataFrame({name: [] for name in names}))

        df = pd.DataFrame([flat for row in rows for flat in _flatten_rows(row)])

        # Aggregation is the only thing computed on the client
        if has_agg:
            aggregated = _apply_aggregation(df, columns, group_by)
            if order_by:
                aggregated = _sort_aggregated(aggregated, order_by, group_by)
            aggregated = aggregated.rename(columns=renames)
            aggregated = aggregated.drop_duplicates().reset_index(drop=True) if distinct else aggregated
            return GraphQLOutput(_slice_rows(aggregated, offset, limit))

        if sort_expanded:
            df = _sort_expanded(df, order_by or [])
        projected = _project_columns(df, columns, names)
        if distinct:  # the limit is applied here, after the duplicates are gone, not on the server before
            projected = projected.drop_duplicates().reset_index(drop=True)
        return GraphQLOutput(_slice_rows(projected, offset, limit) if distinct or offset else projected)
