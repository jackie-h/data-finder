import datetime
from dataclasses import replace
from typing import Any, Callable, Literal, Sequence, Union, overload

from datafinder import Attribute, Operation, DataFrame
from datafinder.relation_planner import (
    _attributes, alias_of, attributes_of, parts_of, relation_numbers as _relation_numbers)
from datafinder.sql_generator import to_sql, view_sql_to_string, display_alias, check_view_joins
from model.milestoning import ProcessingTemporalColumns, milestoning_columns_of
from model.naming import to_snake_case
from model.relation_tree import (
    Aggregate, Concatenate, Distinct, Drop, Extend, Filter, Join, Project, Read, RelationNode, Select, Sort, Take)
from model.relational import (
    Alias, JoinKind, NoOperation, Relation, RelationExistsOperation, SortOperation, Table, TableAlias)
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



class Row:
    """A row of a result as a function of it reads it: its columns are attributes, ``r.price``. The
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
    """Whether ``other`` has a row for which ``condition`` holds, as SQL's ``EXISTS``. It is a
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
        """The keys, and a column for each keyword that is an aggregate of the rows of its group::

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
        return self._result._step(Aggregate, [*self._keys, *aggregates], group_by=keys)


class FinderResult(DataFrame):
    """The rows of a query, as what was written of it: a tree of steps (``model.relation_tree``), of which this is the last. A step
    gives a result of its own and leaves this one as it was. Nothing is worked out until the SQL, or the rows, are asked for."""

    def __init__(self, business_date: datetime.date | None,
                 processing_datetime: datetime.datetime | datetime.date | None,
                 columns: Sequence, table: Relation, op: Operation,
                 business_date_to: datetime.date | None = None,
                 processing_datetime_to: datetime.datetime | datetime.date | None = None,
                 *, node: RelationNode | None = None):
        self._node: RelationNode = node if node is not None else Read(
            columns, table, op, business_date, processing_datetime, business_date_to, processing_datetime_to)
        self._timeout_ms: int = _DEFAULT_TIMEOUT_MS

    def _then(self, node: RelationNode) -> 'FinderResult':
        """A result of a step written after this one."""
        result = FinderResult(None, None, (), None, NoOperation(), node=node)  # type: ignore[arg-type]
        result._timeout_ms = self._timeout_ms
        parts_of(node)  # a step that cannot be said is refused when it is written, not when the SQL is asked for
        return result

    # --- a result as a relation, for what follows it ---------------------------------------------------------------

    def _relation(self) -> tuple[TableAlias, dict[str, Any]]:
        """This result as a relation: its query, with each column named for what reads it."""
        return alias_of(self._node), attributes_of(self._node)

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
        ``UNION ALL`` with ``all=True``. The result is a result like any other: it can be
        ordered, limited and concatenated again. Its columns are named as ``columns()`` gives them."""
        left, right = self.columns(), other.columns()
        if left != right:
            raise ValueError(f"Concatenated results need the same columns: {left} and {right} differ")
        return self._then(Concatenate(self._node, other._node, all))

    def _row(self) -> 'Row':
        return Row(self._relation()[1])

    def _step(self, kind: type[Project], display: Sequence, op: Operation | None = None, group_by: Sequence = ()) -> 'FinderResult':
        return self._then(kind(self._node, display, op, tuple(group_by)))

    def filter(self, *conditions: Callable[['Row'], Operation]) -> 'FinderResult':
        """The rows of this result for which every condition holds: each
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
        return self._step(Filter, list(self._relation()[1].values()), op)

    def extend(self, **columns: Callable[['Row'], Any]) -> 'FinderResult':
        """This result with more columns, each named by its keyword and worked out from a row.
        A window adds an aggregate over the rows to each row::

            rows.extend(double=lambda r: r.price * 2.0,
                        top=lambda r: r.price.max().over(partition_by=[r.account]))
        """
        row = self._row()
        for name, expression in columns.items():
            if not callable(expression):
                raise TypeError(f"The column '{name}' is a function of a row: extend({name}=lambda r: ...)")
        return self._step(Extend, [*self._relation()[1].values(),
                                   *(Alias(expression(row), name) for name, expression in columns.items())])

    def select(self, *names: str) -> 'FinderResult':
        """Only some of this result's columns, by name. To rename or work out a
        column use ``extend`` first."""
        _, attributes = self._relation()
        missing = [n for n in names if n not in attributes]
        if missing:
            raise KeyError(f"No column {missing} in this result: its columns are {list(attributes)}")
        return self._step(Select, [attributes[n] for n in names])

    def join(self, other: 'FinderResult', condition: Callable[['Row', 'Row'], Operation],
             how: Literal["inner", "left", "right", "full"]) -> 'FinderResult':
        """The rows of this result put together with the rows of ``other``. The condition is a function
        of a row of each (bracket each comparison, and join them with ``&`` or ``|``: ``and`` and ``or`` would drop one)::

            orders.join(customers, lambda o, c: o.customer_id == c.id, how="inner")     # only orders that have a customer
            orders.join(customers, lambda o, c: o.customer_id == c.id, how="left")      # every order, with a customer or NULL
            lines.join(others, lambda a, b: (a.order == b.order) & (a.supplier != b.supplier), how="inner")

        ``how`` is one of ANSI SQL's join types: ``inner``, or ``left``, ``right`` or ``full``, the outer joins that keep the
        rows that find no match, with NULL for the other side's columns.

        The columns of the two must be named differently: name them with ``alias()``, or ``extend`` then
        ``select``."""
        try:
            kind = JoinKind(how)
        except ValueError:
            raise ValueError(f"A join is one of {[k.value for k in JoinKind]}, not '{how}'") from None
        if not callable(condition):
            raise TypeError("A join condition is a function of a row of each: join(other, lambda a, b: a.key == b.key)")
        _, left = self._relation()
        _, right = other._relation()
        repeated = sorted(set(left) & set(right))
        if repeated:
            raise ValueError(f"A join needs the columns of its two sides named differently, but {repeated} are on both: "
                             "name them with alias(), or extend() then select()")
        test = condition(Row(left), Row(right))
        if not isinstance(test, Operation):
            raise TypeError("A join condition gives a comparison, such as a.key == b.key, not "
                            f"{type(test).__name__} (combine comparisons with & and |, each in brackets)")
        columns = [Alias(a, n) for n, a in (*left.items(), *right.items())]
        return self._then(Join(self._node, other._node, kind, test, columns))

    @overload
    def group_by(self, *keys: Callable[['Row'], Any]) -> 'GroupedRows': ...  # pyright: ignore[reportOverlappingOverload]
    @overload
    def group_by(self, *keys: Any) -> 'FinderResult': ...
    def group_by(self, *keys) -> 'GroupedRows | FinderResult':
        """Group the rows. Given finder attributes, the aggregates selected next to them are grouped by them, and the result
        is this one. Given a function of a row, the rows of this result are grouped by
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
        return self._then(replace(self._read("group_by"), group_by=tuple(keys)))

    def _read(self, name: str) -> Read:
        """The finder's own query, which says what it groups and keeps by. That is of the rows it reads, so it cannot follow a step."""
        if not isinstance(self._node, Read):
            raise ValueError(f"{name} is said of the rows a finder reads, so it comes before every other step: write {name} first, or "
                             f"group the rows after the steps with group_by(lambda r: ...).aggregate(...)")
        return self._node

    def aggregate(self, **columns: Callable[['Row'], Any]) -> 'FinderResult':
        """One row for the whole of this result, each column named by its keyword and an aggregate of the rows:
        ``rows.aggregate(total=lambda r: r.salary.sum())``."""
        return GroupedRows(self, ()).aggregate(**columns)

    def order_by(self, *sort_ops: SortOperation) -> 'FinderResult':
        """Order the rows. Calls accumulate: the sorts of the latest call take priority and earlier ones order the
        rows it leaves tied, as in ``df.sort_values(a).sort_values(b)``. Sorting a column again replaces its earlier
        direction. Rows that are already dropped or taken are ordered after that, as a step over them."""
        return self._then(Sort(self._node, tuple(sort_ops)))

    def limit(self, n: int) -> 'FinderResult':
        """The first ``n`` rows. Taking again takes from the rows taken."""
        return self._then(Take(self._node, n))

    def offset(self, n: int) -> 'FinderResult':
        """Skip the first ``n`` rows. Sort the rows first: without an order, which rows come first is not defined. Rows that were
        already taken are skipped after that: ``limit(5).offset(2)`` is three rows."""
        if n < 0:
            raise ValueError("offset cannot be negative")
        return self._then(Drop(self._node, n))

    def slice(self, start: int, stop: int) -> 'FinderResult':
        """Rows ``start`` up to but not including ``stop``, counting from 0: ``offset(start).limit(stop - start)``."""
        if start < 0 or stop < start:
            raise ValueError("slice needs 0 <= start <= stop")
        return self.offset(start).limit(stop - start)

    def having(self, *conditions: Operation) -> 'FinderResult':
        """Keep the groups for which every condition holds: ``HAVING``. A condition tests an aggregate or a grouping
        column: ``f.find_all([f.category(), count_all()]).group_by(f.category()).having(count_all() > 1)``.
        Calls accumulate, as ``and_`` does."""
        read = self._read("having")
        having = read.having
        for condition in conditions:
            having = condition if having is None else having.and_(condition)
        return self._then(replace(read, having=having))

    def distinct(self) -> 'FinderResult':
        """Return each distinct row once: ``SELECT DISTINCT``."""
        return self._then(Distinct(self._node))

    def timeout(self, milliseconds: int) -> 'FinderResult':
        self._timeout_ms = milliseconds
        return self

    def _execute(self) -> DataFrame:
        p = parts_of(self._node)
        return QueryRunnerBase.get_runner().select(
            p.business_date, p.processing_datetime, p.columns, p.table, p.op, list(p.order_by), list(p.group_by), p.limit,
            self._timeout_ms, business_date_to=p.business_date_to, distinct=p.distinct, offset=p.offset,
            having=p.having, processing_datetime_to=p.processing_datetime_to,
        )

    def to_view(self, name: str, schema: str) -> 'ViewDefinition':
        """Define a SQL view from this query: ``schema.name``.

        A view has no dates, so ``find_all`` must have been called with ``None`` for the business date and
        processing time. The view exposes the root table's milestoning columns instead, and every joined
        milestoned table is pinned to the version that applied when the root row was recorded. A view also
        has no ORDER BY or row limit; to order the rows you read back use ``ViewDefinition.result().order_by()``.
        Each column is named after its display name in snake_case, e.g. "Account Name" becomes ``account_name``.
        """
        p = parts_of(self._node)
        if (p.business_date is not None or p.business_date_to is not None
                or p.processing_datetime is not None or p.processing_datetime_to is not None):
            raise ViewDefinitionError(
                "A view has no dates: pass None for the business date and processing time, and the view will "
                "expose the milestoning columns instead")
        if p.distinct:
            raise ViewDefinitionError("A view cannot be distinct")
        if p.offset is not None:
            raise ViewDefinitionError("A view has no row offset")
        if p.having is not None:
            raise ViewDefinitionError("A view has no HAVING; filter the rows you read back from it")
        if p.order_by or p.limit is not None:
            raise ViewDefinitionError(
                "A view has no ORDER BY or row limit; order the rows when you read them with result().order_by()")
        if not isinstance(p.table, Table):
            raise ViewDefinitionError("A view is defined over a table, not over a union")
        projection = [Alias(column, to_snake_case(display_alias(column))) for column in p.columns]
        view = ViewDefinition(name, schema, p.table, projection, p.op, list(p.group_by))
        check_view_joins(view)
        return view

    def to_sql(self) -> str:
        p = parts_of(self._node)
        return to_sql(
            p.business_date, p.processing_datetime,
            p.columns, p.table, p.op,
            list(p.order_by), list(p.group_by), p.limit,
            business_date_to=p.business_date_to, distinct=p.distinct, offset=p.offset,
            having=p.having, processing_datetime_to=p.processing_datetime_to,
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

