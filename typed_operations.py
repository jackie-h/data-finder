from operation import BaseOperation, Operation, QueryEngine
from attribute import Attribute


class EqOperation(BaseOperation):
    __attribute: Attribute

    def column_type(self) -> str:
        return self.__attribute._column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute._column_name() + ' = ' + self.prepare_value())

    def prepare_value(self) -> str:
        pass


class PrimitiveEqOperation(EqOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return str(self.__value)


class StringEqOperation(EqOperation):
    __value: str

    def __init__(self, attrib: Attribute, value: str):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        if self.column_type() == 'kx_symbol':
            return "`" + self.__value
        else:
            return "\"" + self.__value + "\""
