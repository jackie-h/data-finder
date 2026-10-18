import re
from typing import Sequence

from model.milestoning import MilestonedTable
from model.relational import QueryExpression, Relation, Alias, Table, Column, Operation, NoOperation, \
    LogicalOperation, ComparisonOperation, UnaryOperation, AggregateOperation, WindowFunctionOperation, \
    CountAllOperation

_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class ViewDefinitionError(ValueError):
    """Raised when a view definition can't be made reproducible or unambiguous."""


def _check_identifier(kind: str, value: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ViewDefinitionError(f"{kind} {value!r} must be a plain identifier (letters, digits, underscores)")


def _contains_aggregate_or_window(op) -> bool:
    """True when ``op`` holds an aggregate, window function or count."""
    if isinstance(op, (AggregateOperation, WindowFunctionOperation, CountAllOperation)):
        return True
    if isinstance(op, (LogicalOperation, ComparisonOperation)):
        return _contains_aggregate_or_window(op.left) or _contains_aggregate_or_window(op.right)
    if isinstance(op, (UnaryOperation, Alias)):
        return _contains_aggregate_or_window(op.element)
    return False


def _contains_aggregate(op) -> bool:
    """True when ``op`` collapses rows: an aggregate or a count. Window functions keep every row."""
    if isinstance(op, (AggregateOperation, CountAllOperation)):
        return True
    if isinstance(op, (LogicalOperation, ComparisonOperation)):
        return _contains_aggregate(op.left) or _contains_aggregate(op.right)
    if isinstance(op, (UnaryOperation, Alias)):
        return _contains_aggregate(op.element)
    return False


class View(QueryExpression, Relation):
    """A SQL view definition: a named relation defined by a query expression over a root table, with no dates.

    A view can hold what ANSI SQL allows in a view's query expression, which is everything a
    QueryExpression has: joins, a filter, grouping, aggregates and window functions. It has no
    ORDER BY or row limit.

    Its columns are the projection plus the root table's milestoning columns, so consumers choose the point in
    time when they query it. If the view is grouped, the root's milestoning columns are grouped by too, so each
    group is a set of rows with the same validity.

    Construction checks what can be checked from the definition alone. Whether its joins can be pinned to the
    root row's point in time depends on how the joins are resolved, so that is checked by the SQL generator
    (see datafinder.sql_generator.check_view_joins).
    """

    def __init__(self, name: str, schema: str, table: Table, projection: Sequence[Alias],
                 filter: Operation | None = None, group_by: list | None = None):
        _check_identifier('View name', name)
        _check_identifier('Schema name', schema)
        Relation.__init__(self)
        self.projection = tuple(projection)
        milestoning = table.milestoning_columns.columns() if isinstance(table, MilestonedTable) else []
        milestoning_aliases = [Alias(Column(c.name, c.type, c.qualified_name()), c.name) for c in milestoning]
        self._columns = [*self.projection, *milestoning_aliases]

        group_by = list(group_by or [])
        if group_by or any(_contains_aggregate(column) for column in self.projection):
            group_by += [alias.element for alias in milestoning_aliases]
        QueryExpression.__init__(self, self._columns, NoOperation() if filter is None else filter, group_by, table)
        self.name = name
        self.schema = schema
        self._validate()

    @property
    def qualified_name(self) -> str:
        return self.schema + '.' + self.name

    def column_names(self) -> list[str]:
        """Projected column names followed by the root's milestoning columns."""
        return [alias.name for alias in self._columns]

    def _validate(self) -> None:
        if not self.projection:
            raise ViewDefinitionError("A view needs at least one projected column")
        for column in self.projection:
            if not isinstance(column, Alias):
                raise ViewDefinitionError(f"Every projected column needs a name, not a bare {type(column).__name__}")
        if _contains_aggregate_or_window(self.filter):
            raise ViewDefinitionError("A view's filter can't contain aggregates, windows or counts")
        if any(_contains_aggregate_or_window(expression) for expression in self.group_by):
            raise ViewDefinitionError("A view can't group by aggregates, windows or counts")

        names = [n.lower() for n in self.column_names()]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ViewDefinitionError(f"Projected columns resolve to the same name: {duplicates}")
