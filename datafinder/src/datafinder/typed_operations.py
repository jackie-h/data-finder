import datetime

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
        return "'" + self.__value + "'"

def sql_format_datetime(value:datetime.datetime):
    return value.strftime("'%Y-%m-%d %H:%M:%S'")

def sql_format_date(value:datetime.date):
    return value.strftime("'%Y-%m-%d'")


class DateEqOperation(PrimitiveEqOperation):

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib, value)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_date(self.__value)


class DateTimeEqOperation(PrimitiveEqOperation):

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib, value)

    def prepare_value(self) -> str:
        return sql_format_datetime(self.__value)


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

class DateGreaterThanOperation(GreaterThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_date(self.__value)


class DateTimeGreaterThanOperation(GreaterThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_datetime(self.__value)


class GreaterThanOrEqualToOperation(BaseOperation):
    __attribute: Attribute

    def column_type(self) -> str:
        return self.__attribute.column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute, '>=', self.prepare_value())

    def prepare_value(self) -> str:
        pass


class PrimitiveGreaterThanOrEqualToOperation(GreaterThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return str(self.__value)


class DateGreaterThanOrEqualToOperation(GreaterThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_date(self.__value)


class DateTimeGreaterThanOrEqualToOperation(GreaterThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_datetime(self.__value)


class LessThanOperation(BaseOperation):
    __attribute: Attribute

    def column_type(self) -> str:
        return self.__attribute.column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute, '<', self.prepare_value())

    def prepare_value(self) -> str:
        pass


class PrimitiveLessThanOperation(LessThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return str(self.__value)


class DateLessThanOperation(LessThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_date(self.__value)


class DateTimeLessThanOperation(LessThanOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_datetime(self.__value)


class LessThanOrEqualToOperation(BaseOperation):
    __attribute: Attribute

    def column_type(self) -> str:
        return self.__attribute.column_type()

    def __init__(self, attrib: Attribute):
        self.__attribute = attrib

    def generate_query(self, query: QueryEngine):
        query.append_where_clause(self.__attribute, '<=', self.prepare_value())

    def prepare_value(self) -> str:
        pass


class PrimitiveLessThanOrEqualToOperation(LessThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return str(self.__value)


class DateLessThanOrEqualToOperation(LessThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value:datetime.datetime):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_date(self.__value)


class DateTimeLessThanOrEqualToOperation(LessThanOrEqualToOperation):
    __value: []

    def __init__(self, attrib: Attribute, value:datetime.datetime):
        super().__init__(attrib)
        self.__value = value

    def prepare_value(self) -> str:
        return sql_format_datetime(self.__value)
