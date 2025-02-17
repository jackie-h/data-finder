from .operation import BaseOperation, QueryEngine
from .attribute import Attribute


class EqOperation(BaseOperation):
    __attribute: Attribute

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def attribute(self) -> Attribute:
        return self.__attribute

    def column_type(self) -> str:
        return self.__attribute.column_type()

    def column_name(self) -> str:
        return self.__attribute.column_name()

    def prepare_value(self) -> str:
        pass


class PrimitiveEqOperation(EqOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.attribute(), '=', self.prepare_value())

    def prepare_value(self) -> str:
        return str(self.__value)


class StringEqOperation(EqOperation):
    __value: str

    def __init__(self, attrib: Attribute, value: str):
        super().__init__(attrib)
        self.__value = value

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.attribute(), 'LIKE', self.prepare_value())

    def prepare_value(self) -> str:
        #TODO - String escaper
        if self.column_type() == 'kx_symbol':
            return "`" + self.__value
        else:
            return "'" + self.__value + "'"


class GreaterThanOperation(BaseOperation):
    __attribute: Attribute

    def column_type(self) -> str:
        return self.__attribute.column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute, '>', self.prepare_value())

    def prepare_value(self) -> str:
        pass


class PrimitiveGreaterThanOperation(GreaterThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return str(self.__value)
