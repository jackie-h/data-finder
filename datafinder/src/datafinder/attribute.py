from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement


class Attribute:
    __column: Column
    __owner: str
    __parent: Any

    def __init__(self, name: str, column_db_type: str, owner:str, parent=None):
        self.__column = Column(name, column_db_type, owner)
        self.__owner = owner
        self.__parent = parent

    def column(self) -> Column:
        return self.__column

    def owner(self) -> str:
        return self.__owner

    def parent(self) -> Any:
        return self.__parent
