from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement, \
    ColumnWithJoin, SortOperation, SortDirection, AggregateOperation, AggregateOperator, IsNullOperation, IsNotNullOperation


class Attribute:
    """Base wrapper for a table column exposed through the finder API."""

    __display_name: str
    __column: Column
    __owner: str
    __parent: Any

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        """Create a typed attribute for a named column."""
        self.__display_name = display_name
        self.__column = Column(column_name, column_db_type, owner)
        self.__owner = owner
        self.__parent = parent

    def column(self) -> Column:
        """Return the underlying column metadata."""
        return self.__column

    def owner(self) -> str:
        """Return the owning table or relation name."""
        return self.__owner

    def parent(self) -> Any:
        """Return the parent join, if this attribute is traversal-based."""
        return self.__parent

    def display_name(self) -> str:
        """Return the human-readable display name."""
        return self.__display_name

    def count(self) -> AggregateOperation:
        """Return a count aggregation for this Attribute."""
        return AggregateOperation(ColumnWithJoin(self.__column, self.__parent), AggregateOperator.COUNT, self.__display_name + ' Count')

    def ascending(self) -> SortOperation:
        """Return an ascending sort operation for this Attribute."""
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.ASC)

    def descending(self) -> SortOperation:
        """Return a descending sort operation for this Attribute."""
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.DESC)

    def is_none(self) -> Operation:
        """Return a filter that matches rows where this column is NULL. Equivalent to ``is None`` in Python."""
        return IsNullOperation(ColumnWithJoin(self.__column, self.__parent))

    def is_not_none(self) -> Operation:
        """Return a filter that matches rows where this column is not NULL. Equivalent to ``is not None`` in Python."""
        return IsNotNullOperation(ColumnWithJoin(self.__column, self.__parent))
