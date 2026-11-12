import datetime
import decimal
import itertools
from typing import Any, Callable, Literal, Optional, Sequence, Union, overload

from datafinder import Attribute, Operation, DataFrame
from datafinder.sql_generator import to_sql, view_sql_to_string, display_alias, check_view_joins, build_query_specification
from datafinder.typed_attributes import typed_attribute
from model.naming import to_snake_case
from model.milestoning import MilestonedTable, ProcessingTemporalColumns, milestoning_columns_of
from model.relational import (
    AggregateOperation, AggregateOperator, Alias, ArithmeticOperation, BooleanConstantOperation, CaseOperation, Column,
    ColumnWithJoin, ConcatOperation, ConstantOperation, CountAllOperation, DateArithmeticOperation, DateConstantOperation,
    DateDiffOperation, DateExtractOperation, DateTimeConstantOperation, DateTruncOperation, DecimalConstantOperation,
    FloatConstantOperation, IntegerConstantOperation, IntervalConstantOperation, IntervalOperation, JoinKind, JoinOperation, NoOperation, Operation,
    QueryExpression, QuerySpecification, Relation, RelationExistsOperation, RelationalOperationElement,
    ScalarFunctionOperation, SortOperation, StringConstantOperation, StringFunctionOperation, Table, TableAlias,
    UnionOperation,
    WindowFunctionOperation)
from model.view import View, ViewDefinitionError, contains_aggregate


#TODO revisit this, don't want this to be static per class as need to be able to switch them
class RegistryBase(type):
    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        # instantiate a new type corresponding to the type of class being defined
        # this is currently RegisterBase but in child classes will be the child class
        new_cls = type.__new__(cls, name, bases, attrs)
        cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)

    @classmethod
    def register(cls, clazz):
        cls.REGISTRY[clazz.__name__] = clazz

    @classmethod
    def clear(cls):
        RegistryBase.REGISTRY = {}

def convert_date_time(maybe_datetime) -> datetime.datetime | datetime.date:
    """Convert a processing date input. A date (or "%Y-%m-%d" string) stays a date, so it is rendered as a date
    literal in SQL; a datetime (or "%Y-%m-%d %H:%M:%S" string) stays a timestamp."""
    if isinstance(maybe_datetime, str):
        for fmt, to_value in (("%Y-%m-%d %H:%M:%S", lambda dt: dt), ("%Y-%m-%d", lambda dt: dt.date())):
            try:
                return to_value(datetime.datetime.strptime(maybe_datetime, fmt))
            except ValueError:
                pass
        raise ValueError("Processing date string must be in the format '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'")
    elif isinstance(maybe_datetime, datetime.datetime):  # before date: datetime is a subclass of date
        return maybe_datetime
    elif isinstance(maybe_datetime, datetime.date):
        return maybe_datetime
    else:
        raise TypeError("Input must be a string, a datetime object or a date object.")

def convert_date(maybe_date) -> datetime.date:
    if isinstance(maybe_date, str):
        # Assume a specific format for string input, or try multiple formats
        try:
            dt = datetime.datetime.strptime(maybe_date, "%Y-%m-%d")
            return dt.date()
        except ValueError:
            raise ValueError("Date time string must be in the format '%Y-%m-%d'")
    elif isinstance(maybe_date, datetime.date):
        return maybe_date
    else:
        raise TypeError("Input must be a string or a date object.")


_DEFAULT_TIMEOUT_MS = 60_000

_relation_numbers = itertools.count(1)


def _sort_target(sort_op: SortOperation) -> tuple:
    """What a sort orders by: a column and the joins to reach it. Any other expression, such as an aggregate, is its
    own target, so sorting by an expression again adds a sort."""
    column = sort_op.column
    if not isinstance(column, ColumnWithJoin):
        return ("expression", id(column))
    joins = []
    node = column.parent
    while node is not None:
        joins.append(node.join.name)
        node = node.parent
    return column.column.owner_name, column.column.name, tuple(joins)


# --- the columns of a result, for a query over it -------------------------------------------------------------------

_NUMBER_TYPES = {int: "BIGINT", float: "DOUBLE", decimal.Decimal: "DECIMAL"}

_CONSTANT_TYPES = {
    IntegerConstantOperation: "BIGINT", FloatConstantOperation: "DOUBLE", StringConstantOperation: "VARCHAR",
    DateConstantOperation: "DATE", DateTimeConstantOperation: "TIMESTAMP", DecimalConstantOperation: "DECIMAL",
    BooleanConstantOperation: "BOOLEAN", IntervalConstantOperation: "INTERVAL",
}


def _column_name(element: RelationalOperationElement) -> str:
    """The name a column of a result has for the step after it: the name it was given, or its display name in snake_case
    (as a view names its columns)."""
    return element.name if isinstance(element, Alias) else to_snake_case(display_alias(element))


def _column_type(element: RelationalOperationElement) -> str:
    """The database type of what a result column holds, so a query over the result can compare it as that type."""
    if isinstance(element, Alias):
        return _column_type(element.element)
    if isinstance(element, Attribute):
        return element.column().type
    if isinstance(element, ColumnWithJoin):
        return element.column.type
    if isinstance(element, Column):
        return element.type
    if isinstance(element, CountAllOperation):
        return "BIGINT"
    if isinstance(element, IntervalOperation):  # before the number kinds: it is arithmetic's subclass, but not a number
        return "INTERVAL"
    # a number of a known kind: an integer, double or decimal arithmetic result, function, aggregate or constant
    kind = getattr(element, "_number_kind", None)
    if kind in _NUMBER_TYPES:
        return _NUMBER_TYPES[kind]
    if isinstance(element, AggregateOperation):
        if element.operator in (AggregateOperator.COUNT, AggregateOperator.COUNT_DISTINCT):
            return "BIGINT"
        if element.operator == AggregateOperator.STRING_AGG:
            return "VARCHAR"
        if element.operator in (AggregateOperator.MIN, AggregateOperator.MAX, AggregateOperator.SUM):
            return _column_type(element.element)
        if element.operator in (AggregateOperator.BOOL_AND, AggregateOperator.BOOL_OR):
            return "BOOLEAN"
        return "DOUBLE"
    if isinstance(element, WindowFunctionOperation):
        return _column_type(element.element) if element.element is not None else "BIGINT"
    if isinstance(element, ConstantOperation):
        return _CONSTANT_TYPES.get(type(element), "VARCHAR")
    if isinstance(element, CaseOperation):
        return _column_type(element.then)
    if isinstance(element, (ConcatOperation, StringFunctionOperation)):
        return "VARCHAR"
    if isinstance(element, (DateExtractOperation, DateDiffOperation)):
        return "BIGINT"
    if isinstance(element, (DateArithmeticOperation, DateTruncOperation)):
        return _column_type(element.element)
    if isinstance(element, ScalarFunctionOperation):  # an untyped one, such as fill_null: what it is applied to
        return _column_type(element.element)
    if isinstance(element, ArithmeticOperation):  # one of unknown kind
        return "DOUBLE"
    return "VARCHAR"


def _attributes(alias: TableAlias) -> dict[str, Attribute]:
    """The columns of a relation under a name, as a query that reads from it sees them: a typed attribute for each, named
    as ``_column_name`` names it and owned by the alias."""
    names = [_column_name(element) for element in alias.columns]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"A result needs each column named once, but {duplicates} are repeated: name them with alias()")
    return {name: typed_attribute(name, _column_type(element), alias) for name, element in zip(names, alias.columns)}


class Row:
    """A row of a result as a function of it reads it: its columns are attributes, ``r.price``, as Legend's ``$r.price``. The
    columns are known when the query is built, so a name that is not one is refused then, with the columns there are."""

    def __init__(self, attributes: dict[str, Attribute]):
        object.__setattr__(self, "_columns", attributes)

    def __getattr__(self, name: str) -> Any:
        columns = object.__getattribute__(self, "_columns")
        if name not in columns:
            raise AttributeError(f"No column '{name}' in this result: its columns are {list(columns)}")
        return columns[name]

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("A row is read, not changed: use extend() to add a column")


def exists(other: 'FinderResult', condition: Callable[['Row'], Operation]) -> RelationExistsOperation:
    """Whether ``other`` has a row for which ``condition`` holds, as Legend's ``exists(e | ...)``, and SQL's ``EXISTS``. It is a
    condition, for the ``filter`` of a result: the function is of a row of ``other``, and it reads the row being filtered from
    the function it is in. ``~exists(...)`` is ``NOT EXISTS``, and conditions are combined with ``&`` and ``|``::

        late_lines.filter(lambda l1: exists(lines, lambda l2: (l2.order == l1.order) & (l2.supplier != l1.supplier))
                                     & ~exists(late, lambda l3: (l3.order == l1.order) & (l3.supplier != l1.supplier)))
    """
    if not callable(condition):
        raise TypeError("The condition of an exists is a function of a row of the other result: exists(other, lambda o: o.key == r.key)")
    relation, _ = other._relation()
    # a name of its own each time, so that a result tested against itself reads as two relations
    inner = TableAlias(relation.relation, f"relation{next(_relation_numbers)}")
    test = condition(Row(_attributes(inner)))
    if not isinstance(test, Operation):
        raise TypeError("The condition of an exists gives a comparison, such as o.key == r.key, not "
                        f"{type(test).__name__} (combine comparisons with & and |, each in brackets)")
    return RelationExistsOperation(inner, test)


class GroupedRows:
    """The rows of a result grouped by keys: what ``group_by(lambda r: r.dept)`` gives, which can only be aggregated."""

    def __init__(self, result: 'FinderResult', keys: Sequence[Callable[['Row'], Any]]):
        self._result = result
        row = result._row()
        self._keys: list = []
        for key in keys:
            found = key(row)
            self._keys.extend(found if isinstance(found, (tuple, list)) else [found])
        for key in self._keys:
            if isinstance(key, Attribute):
                continue
            if isinstance(key, Alias) and not isinstance(key.element, Alias):
                continue
            raise ValueError("A group_by key is a column, or an expression named with alias(): "
                             "group_by(lambda r: r.ship_date.year().alias(\"year\"))")

    def aggregate(self, **columns: Callable[['Row'], Any]) -> 'FinderResult':
        """The keys, and a column for each keyword that is an aggregate of the rows of its group, as Legend's
        ``groupBy(~dept, ~headcount : x | $x.id : y | $y->count())``::

            rows.group_by(lambda r: r.dept).aggregate(headcount=lambda r: r.id.count(),
                                                      avg_salary=lambda r: r.salary.average())
        """
        if not columns:
            raise ValueError("aggregate needs at least one column: aggregate(total=lambda r: r.price.sum())")
        row = self._result._row()
        shown = [key.name if isinstance(key, Alias) else key.display_name() for key in self._keys]
        aggregates: list = []
        for name, function in columns.items():
            if not callable(function):
                raise TypeError(f"The column '{name}' is a function of a row: aggregate({name}=lambda r: r.price.sum())")
            expression = function(row)
            if not contains_aggregate(expression):
                raise TypeError(f"The column '{name}' is not an aggregate of the rows of a group (sum, count, average, min, "
                                "max, ...), so it has no one value for the group")
            if name in shown or any(name == existing.name for existing in aggregates):
                raise ValueError(f"The column '{name}' is named twice in this aggregate")
            aggregates.append(Alias(expression, name))
        keys = [key.element if isinstance(key, Alias) else key for key in self._keys]
        return self._result._next([*self._keys, *aggregates], group_by=keys)


class FinderResult(DataFrame):

    def __init__(self, business_date: datetime.date | None,
                 processing_datetime: datetime.datetime | datetime.date | None,
                 columns: Sequence, table: Relation, op: Operation,
                 business_date_to: datetime.date | None = None,
                 processing_datetime_to: datetime.datetime | datetime.date | None = None):
        self._business_date = business_date
        self._business_date_to = business_date_to
        self._processing_datetime = processing_datetime
        self._processing_datetime_to = processing_datetime_to
        self._columns = columns
        self._table = table
        self._op = op
        self._order_by: list[SortOperation] = []
        self._group_by: list = []
        self._limit: int | None = None
        self._distinct = False
        self._offset: int | None = None
        self._having: Operation | None = None
        self._timeout_ms: int = _DEFAULT_TIMEOUT_MS
        self._relation_name: str | None = None

    # --- a result as a relation, for what follows it ---------------------------------------------------------------

    def _relation(self) -> tuple[TableAlias, dict[str, Any]]:
        """This result as a relation: its query, with each column named for what reads it."""
        if self._relation_name is None:
            self._relation_name = f"relation{next(_relation_numbers)}"
        display = [c if isinstance(c, Alias) else Alias(c, _column_name(c)) for c in self._columns]
        query = build_query_specification(
            self._business_date, self._processing_datetime, display, self._table, self._op, self._order_by, self._group_by,
            self._limit, business_date_to=self._business_date_to, distinct=self._distinct, offset=self._offset,
            having=self._having, processing_datetime_to=self._processing_datetime_to)
        alias = TableAlias(query, self._relation_name)
        return alias, _attributes(alias)

    def columns(self) -> list[str]:
        """The names of this result's columns as what follows it reads them."""
        return list(self._relation()[1])

    def __getitem__(self, name: str) -> Any:
        """A column of this result by name, to order a ``concatenate``d result by: ``rows.order_by(rows["name"].ascending())``."""
        attributes = self._relation()[1]
        if name not in attributes:
            raise KeyError(f"No column '{name}' in this result: its columns are {list(attributes)}")
        return attributes[name]

    def concatenate(self, other: 'FinderResult', all: bool = False) -> 'FinderResult':
        """The rows of this result and of ``other``, which has the same columns: ``UNION`` (each distinct row once), or
        ``UNION ALL`` with ``all=True``, as Legend's ``concatenate``. The result is a result like any other: it can be
        ordered, limited and concatenated again. Its columns are named as ``columns()`` gives them."""
        left_alias, left = self._relation()
        right_alias, right = other._relation()
        if list(left) != list(right):
            raise ValueError(f"Concatenated results need the same columns: {list(left)} and {list(right)} differ")
        alias = TableAlias(UnionOperation(self._union_side(left_alias, all), other._union_side(right_alias, all), all),  # type: ignore[arg-type]
                           f"relation{next(_relation_numbers)}")
        return FinderResult(None, None, [Alias(a, n) for n, a in _attributes(alias).items()], alias, NoOperation())

    def _union_side(self, as_relation: TableAlias, all: bool) -> QueryExpression:
        """This result as one side of a union. A result that is only the union of two more with the same operator is
        that union itself, not a SELECT over it, so ``a.concatenate(b).concatenate(c)`` is one flat chain of unions."""
        source = self._table
        bare = (isinstance(source, TableAlias) and isinstance(source.relation, UnionOperation)
                and source.relation.all == all and isinstance(self._op, NoOperation)
                and not (self._order_by or self._group_by or self._distinct or self._having
                         or self._limit is not None or self._offset is not None))
        return source.relation if bare else as_relation.relation  # type: ignore[union-attr,return-value]

    def _next(self, display: Sequence, op: Operation | None = None, group_by: Sequence = ()) -> 'FinderResult':
        """A result that reads from this one: a query over this result's query, as Legend's relation functions are."""
        alias, _ = self._relation()
        step = FinderResult(None, None, display, alias, NoOperation() if op is None else op)
        step._group_by = list(group_by)
        return step

    def _row(self) -> 'Row':
        return Row(self._relation()[1])

    def filter(self, *conditions: Callable[['Row'], Operation]) -> 'FinderResult':
        """The rows of this result for which every condition holds, as Legend's ``filter(r | $r.price > 250)``: each
        condition is a function of a row that reads its columns as attributes, including one that a window function, an
        aggregate or ``extend`` made, which a ``WHERE`` over the rows themselves cannot do::

            rows.filter(lambda r: r.price > 250.0)
        """
        row = self._row()
        op: Operation | None = None
        for condition in conditions:
            if not callable(condition):
                raise TypeError("A filter condition is a function of a row: filter(lambda r: r.price > 250.0)")
            op = condition(row) if op is None else op.and_(condition(row))
        return self._next(list(self._relation()[1].values()), op)

    def extend(self, **columns: Callable[['Row'], Any]) -> 'FinderResult':
        """This result with more columns, each named by its keyword and worked out from a row, as Legend's
        ``extend(~top : r | ...)``. A window adds an aggregate over the rows to each row::

            rows.extend(double=lambda r: r.price * 2.0,
                        top=lambda r: r.price.max().over(partition_by=[r.account]))
        """
        row = self._row()
        for name, expression in columns.items():
            if not callable(expression):
                raise TypeError(f"The column '{name}' is a function of a row: extend({name}=lambda r: ...)")
        return self._next([*self._relation()[1].values(),
                           *(Alias(expression(row), name) for name, expression in columns.items())])

    def select(self, *names: str) -> 'FinderResult':
        """Only some of this result's columns, by name, as Legend's ``select(~[sym, price])``. To rename or work out a
        column use ``extend`` first."""
        _, attributes = self._relation()
        missing = [n for n in names if n not in attributes]
        if missing:
            raise KeyError(f"No column {missing} in this result: its columns are {list(attributes)}")
        return self._next([attributes[n] for n in names])

    def join(self, other: 'FinderResult', condition: Callable[['Row', 'Row'], Operation],
             how: Literal["inner", "left", "right", "full"]) -> 'FinderResult':
        """The rows of this result put together with the rows of ``other``, as Legend's ``join``. The condition is a function
        of a row of each (bracket each comparison, and join them with ``&`` or ``|``: ``and`` and ``or`` would drop one)::

            orders.join(customers, lambda o, c: o.customer_id == c.id, how="inner")     # only orders that have a customer
            orders.join(customers, lambda o, c: o.customer_id == c.id, how="left")      # every order, with a customer or NULL
            lines.join(others, lambda a, b: (a.order == b.order) & (a.supplier != b.supplier), how="inner")

        ``how`` is one of ANSI SQL's join types: ``inner``, or ``left``, ``right`` or ``full``, the outer joins that keep the
        rows that find no match, with NULL for the other side's columns.

        The columns of the two must be named differently, as Legend requires: name them with ``alias()``, or ``extend`` then
        ``select``."""
        try:
            kind = JoinKind(how)
        except ValueError:
            raise ValueError(f"A join is one of {[k.value for k in JoinKind]}, not '{how}'") from None
        if not callable(condition):
            raise TypeError("A join condition is a function of a row of each: join(other, lambda a, b: a.key == b.key)")
        left_alias, left = self._relation()
        right_alias, right = other._relation()
        repeated = sorted(set(left) & set(right))
        if repeated:
            raise ValueError(f"A join needs the columns of its two sides named differently, but {repeated} are on both: "
                             "name them with alias(), or extend() then select()")
        test = condition(Row(left), Row(right))
        if not isinstance(test, Operation):
            raise TypeError("A join condition gives a comparison, such as a.key == b.key, not "
                            f"{type(test).__name__} (combine comparisons with & and |, each in brackets)")
        columns = [Alias(a, n) for n, a in (*left.items(), *right.items())]
        joined = QuerySpecification(columns, NoOperation(), table=left_alias, joins=[JoinOperation('', right_alias, test, kind)])
        alias = TableAlias(joined, f"relation{next(_relation_numbers)}")
        return FinderResult(None, None, [Alias(a, n) for n, a in _attributes(alias).items()], alias, NoOperation())

    @overload
    def group_by(self, *keys: Callable[['Row'], Any]) -> 'GroupedRows': ...  # pyright: ignore[reportOverlappingOverload]
    @overload
    def group_by(self, *keys: Any) -> 'FinderResult': ...
    def group_by(self, *keys) -> 'GroupedRows | FinderResult':
        """Group the rows. Given finder attributes, the aggregates selected next to them are grouped by them, and the result
        is this one. Given a function of a row, as Legend's ``groupBy(~dept, ...)``, the rows of this result are grouped by
        the key it returns (a column, or a tuple of them; an expression is named with ``alias``), and the result of
        ``aggregate`` on that is a step over this one::

            rows.group_by(lambda r: r.dept).aggregate(headcount=lambda r: r.id.count())
            rows.group_by(lambda r: (r.dept, r.title)).aggregate(total=lambda r: r.salary.sum())
        """
        functions = [callable(key) for key in keys]
        if keys and all(functions):
            return GroupedRows(self, keys)
        if any(functions):
            raise TypeError("group_by takes finder attributes, or functions of a row (lambda r: r.dept), not both")
        self._group_by = list(keys)
        return self

    def aggregate(self, **columns: Callable[['Row'], Any]) -> 'FinderResult':
        """One row for the whole of this result, each column named by its keyword and an aggregate of the rows, as Legend's
        ``aggregate(~total : x | $x.salary : y | $y->plus())``: ``rows.aggregate(total=lambda r: r.salary.sum())``."""
        return GroupedRows(self, ()).aggregate(**columns)

    def _own(self, element):
        """A column of this result, as ``result["name"]`` gives it, is what this result shows under that name: so
        ``result.order_by(result["name"].ascending())`` orders by the column, not by a column of a later query."""
        if isinstance(element, Attribute):
            column = element.column()
        elif isinstance(element, ColumnWithJoin):
            column = element.column
        else:
            return element
        if self._relation_name is not None and isinstance(column.owner, TableAlias) and column.owner.name == self._relation_name:
            shown = {_column_name(c): c.element if isinstance(c, Alias) else c for c in self._columns}
            found = shown.get(column.name, element)
            return found._column_with_join() if isinstance(found, Attribute) else found
        return element

    def order_by(self, *sort_ops: SortOperation) -> 'FinderResult':
        """Order the rows. Calls accumulate: the sorts of the latest call take priority and earlier ones order the
        rows it leaves tied, as in ``df.sort_values(a).sort_values(b)``. Sorting a column again replaces its earlier
        direction."""
        sort_ops = tuple(SortOperation(self._own(s.column), s.direction) for s in sort_ops)
        resorted = {_sort_target(sort_op) for sort_op in sort_ops}
        self._order_by = [*sort_ops, *(s for s in self._order_by if _sort_target(s) not in resorted)]
        return self

    def limit(self, n: int) -> 'FinderResult':
        self._limit = n
        return self

    def offset(self, n: int) -> 'FinderResult':
        """Skip the first ``n`` rows. Sort the rows first: without an order, which rows are first is not defined."""
        if n < 0:
            raise ValueError("offset cannot be negative")
        self._offset = n
        return self

    def slice(self, start: int, stop: int) -> 'FinderResult':
        """Rows ``start`` up to but not including ``stop``, counting from 0: ``offset(start).limit(stop - start)``."""
        if start < 0 or stop < start:
            raise ValueError("slice needs 0 <= start <= stop")
        return self.offset(start).limit(stop - start)

    def having(self, *conditions: Operation) -> 'FinderResult':
        """Keep the groups for which every condition holds: ``HAVING``. A condition tests an aggregate or a grouping
        column: ``f.find_all([f.category(), count_all()]).group_by(f.category()).having(count_all() > 1)``.
        Calls accumulate, as ``and_`` does."""
        for condition in conditions:
            self._having = condition if self._having is None else self._having.and_(condition)
        return self

    def distinct(self) -> 'FinderResult':
        """Return each distinct row once: ``SELECT DISTINCT``."""
        self._distinct = True
        return self

    def timeout(self, milliseconds: int) -> 'FinderResult':
        self._timeout_ms = milliseconds
        return self

    def _execute(self) -> DataFrame:
        return QueryRunnerBase.get_runner().select(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op, self._order_by, self._group_by, self._limit,
            self._timeout_ms, business_date_to=self._business_date_to, distinct=self._distinct, offset=self._offset,
            having=self._having, processing_datetime_to=self._processing_datetime_to,
        )

    def to_view(self, name: str, schema: str) -> 'ViewDefinition':
        """Define a SQL view from this query: ``schema.name``.

        A view has no dates, so ``find_all`` must have been called with ``None`` for the business date and
        processing time. The view exposes the root table's milestoning columns instead, and every joined
        milestoned table is pinned to the version that applied when the root row was recorded. A view also
        has no ORDER BY or row limit; to order the rows you read back use ``ViewDefinition.result().order_by()``.
        Each column is named after its display name in snake_case, e.g. "Account Name" becomes ``account_name``.
        """
        if (self._business_date is not None or self._business_date_to is not None
                or self._processing_datetime is not None or self._processing_datetime_to is not None):
            raise ViewDefinitionError(
                "A view has no dates: pass None for the business date and processing time, and the view will "
                "expose the milestoning columns instead")
        if self._distinct:
            raise ViewDefinitionError("A view cannot be distinct")
        if self._offset is not None:
            raise ViewDefinitionError("A view has no row offset")
        if self._having is not None:
            raise ViewDefinitionError("A view has no HAVING; filter the rows you read back from it")
        if self._order_by or self._limit is not None:
            raise ViewDefinitionError(
                "A view has no ORDER BY or row limit; order the rows when you read them with result().order_by()")
        if not isinstance(self._table, Table):
            raise ViewDefinitionError("A view is defined over a table, not over a union")
        projection = [Alias(column, to_snake_case(display_alias(column))) for column in self._columns]
        view = ViewDefinition(name, schema, self._table, projection, self._op, self._group_by)
        check_view_joins(view)
        return view

    def to_sql(self) -> str:
        return to_sql(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op,
            self._order_by, self._group_by, self._limit,
            business_date_to=self._business_date_to, distinct=self._distinct, offset=self._offset,
            having=self._having, processing_datetime_to=self._processing_datetime_to,
        )

    def to_pandas(self):
        return self._execute().to_pandas()

    def to_numpy(self):
        return self._execute().to_numpy()


class ViewDefinition(View):
    """A relational View that can render itself as DDL and be read back through a ViewResult."""

    def to_sql(self, dialect: str | None = None) -> str:
        """``CREATE OR REPLACE VIEW`` DDL, in generic SQL or transpiled to ``dialect`` (e.g. 'databricks')."""
        return view_sql_to_string(self, dialect)

    def result(self) -> 'ViewResult':
        """The view's rows, read by creating it on the registered runner's backend."""
        return ViewResult(self)


class ViewResult(DataFrame):
    """The rows of a view.

    ``to_sql()`` is the view's CREATE VIEW DDL. ``to_pandas()`` and ``to_numpy()`` have the registered runner
    create the view, then select all of its rows. Like FinderResult, nothing runs until
    one of them is asked for.

    A view has no ORDER BY, so its rows come back in no particular order; ``order_by`` orders the read-back
    (not the view) by output column name.
    """

    def __init__(self, view: View):
        self._view = view
        self._order_by: list[str] = []
        self._timeout_ms: int = _DEFAULT_TIMEOUT_MS

    def order_by(self, *columns: str) -> 'ViewResult':
        unknown = [c for c in columns if c not in self._view.column_names()]
        if unknown:
            raise ValueError(f"Unknown view columns {unknown} — valid columns are: {self._view.column_names()}")
        self._order_by = list(columns)
        return self

    def timeout(self, milliseconds: int) -> 'ViewResult':
        self._timeout_ms = milliseconds
        return self

    def _execute(self) -> DataFrame:
        runner = QueryRunnerBase.get_runner()
        runner.create_view(self._view)
        return runner.select_view(self._view, self._order_by, self._timeout_ms)

    def to_sql(self, dialect: str | None = None) -> str:
        return view_sql_to_string(self._view, dialect)

    def to_pandas(self):
        return self._execute().to_pandas()

    def to_numpy(self):
        return self._execute().to_numpy()


def convert_inputs_and_select(business_date: Union[datetime.date, str] | None,
                              processing_datetime: Union[datetime.datetime, datetime.date, str] | None,
                              columns: Sequence[Attribute], table: Table, op: Operation) -> FinderResult:
    bd = None if business_date is None else convert_date(business_date)
    pdt = None if processing_datetime is None else convert_date_time(processing_datetime)
    return FinderResult(bd, pdt, columns, table, op)


def convert_inputs_and_select_for_date_range(
        business_date_from: Union[datetime.date, str],
        business_date_to: Union[datetime.date, str],
        processing_datetime: Union[datetime.datetime, datetime.date, str],
        columns: Sequence[Attribute], table: Table, op: Operation) -> FinderResult:
    if business_date_from is None:
        raise ValueError("business_date_from is required for find_for_date_range")
    if business_date_to is None:
        raise ValueError("business_date_to is required for find_for_date_range")
    if processing_datetime is None and isinstance(milestoning_columns_of(table), ProcessingTemporalColumns):
        raise ValueError("processing_time is required for find_for_date_range")
    # a table with no processing columns, such as a single business date, has no processing time to give
    bd_from = convert_date(business_date_from)
    bd_to = convert_date(business_date_to)
    pdt = None if processing_datetime is None else convert_date_time(processing_datetime)
    return FinderResult(bd_from, pdt, columns, table, op, business_date_to=bd_to)


def convert_inputs_and_select_for_processing_range(
        business_date: Union[datetime.date, str] | None,
        processing_from: Union[datetime.datetime, datetime.date, str],
        processing_to: Union[datetime.datetime, datetime.date, str],
        columns: Sequence[Attribute], table: Table, op: Operation) -> FinderResult:
    if processing_from is None:
        raise ValueError("processing_from is required for find_for_processing_range")
    if processing_to is None:
        raise ValueError("processing_to is required for find_for_processing_range")
    bd = None if business_date is None else convert_date(business_date)
    return FinderResult(bd, convert_date_time(processing_from), columns, table, op,
                        processing_datetime_to=convert_date_time(processing_to))


class QueryRunnerBase(metaclass=RegistryBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime | datetime.date,
               columns: Sequence[Attribute], table: Relation, op: Operation,
               order_by: list[SortOperation] | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = _DEFAULT_TIMEOUT_MS,
               business_date_to: datetime.date | None = None, distinct: bool = False,
               offset: int | None = None, having=None,
               processing_datetime_to: datetime.datetime | datetime.date | None = None) -> DataFrame:
        raise NotImplementedError()

    @staticmethod
    def create_view(view: View) -> None:
        """Create the view on the backend, replacing any existing view of that name."""
        raise NotImplementedError()

    @staticmethod
    def select_view(view: View, order_by: list[str] | None = None, timeout_ms: int = _DEFAULT_TIMEOUT_MS) -> DataFrame:
        """Return all rows of a view that already exists, ordered by the named view columns."""
        raise NotImplementedError()

    @staticmethod
    def get_runner():
        for k in RegistryBase.REGISTRY.keys():
            if k != 'QueryRunnerBase':
                return RegistryBase.REGISTRY[k]
        raise Exception("No query runner registered")

