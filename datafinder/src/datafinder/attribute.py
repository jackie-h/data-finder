from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement, \
    ColumnWithJoin, SortOperation, SortDirection, AggregateOperation, AggregateOperator


class Attribute:
    __display_name: str
    __column: Column
    __owner: str
    __parent: Any

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        self.__display_name = display_name
        self.__column = Column(column_name, column_db_type, owner)
        self.__owner = owner
        self.__parent = parent

    def column(self) -> Column:
        return self.__column

    def owner(self) -> str:
        return self.__owner

    def parent(self) -> Any:
        return self.__parent

    def display_name(self) -> str:
        return self.__display_name

    def count(self) -> AggregateOperation:
        return AggregateOperation(ColumnWithJoin(self.__column, self.__parent), AggregateOperator.COUNT)

    def ascending(self) -> SortOperation:
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.ASC)

    def descending(self) -> SortOperation:
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.DESC)
