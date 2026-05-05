import datetime
from typing import Union, Optional

from datafinder import Attribute, Operation, DataFrame
from datafinder.sql_generator import to_sql
from model.relational import Table, SortOperation


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

def convert_date_time(maybe_datetime) -> datetime.datetime:
    if isinstance(maybe_datetime, str):
        # Assume a specific format for string input, or try multiple formats
        try:
            return datetime.datetime.strptime(maybe_datetime, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("Date time string must be in the format '%Y-%m-%d %H:%M:%S'")
    elif isinstance(maybe_datetime, datetime.datetime):
        return maybe_datetime
    else:
        raise TypeError("Input must be a string or a datetime object.")

def convert_date(maybe_date) -> datetime.date:
    if isinstance(maybe_date, str):
        # Assume a specific format for string input, or try multiple formats
        try:
            dt = datetime.datetime.strptime(maybe_date, "%Y-%m-%d")
            return dt.date()
        except ValueError:
            raise ValueError("Date time string must be in the format '%Y-%m-%d'")
    elif isinstance(maybe_date, datetime.date):
        return maybe_date
    else:
        raise TypeError("Input must be a string or a date object.")


class FinderResult(DataFrame):

    def __init__(self, business_date: Optional[datetime.date],
                 processing_datetime: Optional[datetime.datetime],
                 columns: list, table: Table, op: Operation):
        self._business_date = business_date
        self._processing_datetime = processing_datetime
        self._columns = columns
        self._table = table
        self._op = op
        self._order_by: list[SortOperation] = []
        self._group_by: list = []
        self._limit: int = None

    def group_by(self, *attrs) -> 'FinderResult':
        self._group_by = list(attrs)
        return self

    def order_by(self, *sort_ops: SortOperation) -> 'FinderResult':
        self._order_by = list(sort_ops)
        return self

    def limit(self, n: int) -> 'FinderResult':
        self._limit = n
        return self

    def _execute(self) -> DataFrame:
        return QueryRunnerBase.get_runner().select(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op, self._order_by, self._group_by, self._limit,
        )

    def to_sql(self) -> str:
        return to_sql(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op,
            self._order_by, self._group_by, self._limit,
        )

    def to_pandas(self):
        return self._execute().to_pandas()

    def to_numpy(self):
        return self._execute().to_numpy()


def convert_inputs_and_select(business_date: Optional[Union[datetime.date, str]],
                              processing_datetime: Optional[Union[datetime.datetime, str]],
                              columns: list[Attribute], table: Table, op: Operation) -> FinderResult:
    bd = None if business_date is None else convert_date(business_date)
    pdt = None if processing_datetime is None else convert_date_time(processing_datetime)
    return FinderResult(bd, pdt, columns, table, op)


class QueryRunnerBase(metaclass=RegistryBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime,
               columns: list[Attribute], table: Table, op: Operation,
               order_by: list[SortOperation] = None, group_by: list = None,
               limit: int = None) -> DataFrame:
        pass

    @staticmethod
    def get_runner():
        for k in RegistryBase.REGISTRY.keys():
            if k != 'QueryRunnerBase':
                return RegistryBase.REGISTRY[k]
        raise Exception("No query runner registered")

