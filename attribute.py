
class Attribute:
    __name: str
    __column_db_type: str

    def __init__(self, name: str, column_db_type: str):
        self.__name = name
        self.__column_db_type = column_db_type

    def _column_name(self) -> str:
        return self.__name

    def _column_type(self) -> str:
        return self.__column_db_type