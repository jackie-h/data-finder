import datetime

from datafinder import Attribute, Operation, DataFrame, DateTimeAttribute, AndOperation, DateAttribute, SelectOperation, \
    NoOperation
from model.relational import Table, SingleBusinessDateColumn, ProcessingTemporalColumns, MilestonedTable


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


class QueryRunnerBase(metaclass=RegistryBase):

    @staticmethod
    def select(business_date:datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation) -> DataFrame:
        pass

    @staticmethod
    def get_runner():
        for k in RegistryBase.REGISTRY.keys():
            if k != 'QueryRunnerBase':
                return RegistryBase.REGISTRY[k]
        raise Exception("No query runner registered")


def build_milestoning_filter_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                               table:MilestonedTable) -> Operation:
    op = None
    #TODO this should not reference attribute
    if isinstance(table.milestoning_columns, ProcessingTemporalColumns):
        ptc:ProcessingTemporalColumns = table.milestoning_columns
        start_at = DateTimeAttribute(ptc.start_at_column.name, ptc.start_at_column.type, ptc.start_at_column.table.name)
        end_at = DateTimeAttribute(ptc.end_at_column.name, ptc.end_at_column.type, ptc.end_at_column.table.name)
        op = AndOperation(start_at <= processing_datetime,(end_at > processing_datetime))
    elif isinstance(table.milestoning_columns, SingleBusinessDateColumn):
        sbdc:SingleBusinessDateColumn = table.milestoning_columns
        business_att = DateAttribute(sbdc.business_date_column.name, sbdc.business_date_column.type, sbdc.business_date_column.table.name)
        op = business_att == business_date
    return op

def build_query_operation(business_date:datetime.date, processing_datetime: datetime.datetime,
                         columns: list[Attribute], table: Table, op: Operation) -> SelectOperation:
    if isinstance(table, MilestonedTable):
        milestoned_op = build_milestoning_filter_operation(business_date, processing_datetime, table)
        op = milestoned_op if isinstance(op, NoOperation) else AndOperation(op, milestoned_op)
    select = SelectOperation(columns, table.name, op)
    return select