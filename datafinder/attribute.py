from typing import Any


class Attribute:
    __name: str
    __column_db_type: str
    __owner: str
    __parent: Any

    def __init__(self, name: str, column_db_type: str, owner:str, parent=None):
        self.__name = name
        self.__column_db_type = column_db_type
        self.__owner = owner
        self.__parent = parent

    def column_name(self) -> str:
        return self.__name

    def column_type(self) -> str:
        return self.__column_db_type

    def owner(self) -> str:
        return self.__owner

    def parent(self) -> Any:
        return self.__parent
