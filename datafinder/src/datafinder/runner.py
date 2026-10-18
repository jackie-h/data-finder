import datetime
import re
from typing import Union, Optional

from datafinder import Attribute, Operation, DataFrame
from datafinder.sql_generator import to_sql, view_sql_to_string, display_alias, check_view_joins
from model.relational import Table, SortOperation, Alias
from model.view import View, ViewDefinitionError


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


_DEFAULT_TIMEOUT_MS = 60_000


class FinderResult(DataFrame):

    def __init__(self, business_date: datetime.date | None,
                 processing_datetime: datetime.datetime | None,
                 columns: list, table: Table, op: Operation,
                 business_date_to: datetime.date | None = None):
        self._business_date = business_date
        self._business_date_to = business_date_to
        self._processing_datetime = processing_datetime
        self._columns = columns
        self._table = table
        self._op = op
        self._order_by: list[SortOperation] = []
        self._group_by: list = []
        self._limit: int | None = None
        self._timeout_ms: int = _DEFAULT_TIMEOUT_MS

    def group_by(self, *attrs) -> 'FinderResult':
        self._group_by = list(attrs)
        return self

    def order_by(self, *sort_ops: SortOperation) -> 'FinderResult':
        self._order_by = list(sort_ops)
        return self

    def limit(self, n: int) -> 'FinderResult':
        self._limit = n
        return self

    def timeout(self, milliseconds: int) -> 'FinderResult':
        self._timeout_ms = milliseconds
        return self

    def _execute(self) -> DataFrame:
        return QueryRunnerBase.get_runner().select(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op, self._order_by, self._group_by, self._limit,
            self._timeout_ms, business_date_to=self._business_date_to,
        )

    def to_view(self, name: str, schema: str) -> 'ViewDefinition':
        """Define a SQL view from this query: ``schema.name``.

        A view has no dates, so ``find_all`` must have been called with ``None`` for the business date and
        processing time. The view exposes the root table's milestoning columns instead, and every joined
        milestoned table is pinned to the version that applied when the root row was recorded. A view also
        has no ORDER BY or row limit; to order the rows you read back use ``ViewDefinition.result().order_by()``.
        Each column is named after its display name in snake_case, e.g. "Account Name" becomes ``account_name``.
        """
        if self._business_date is not None or self._business_date_to is not None or self._processing_datetime is not None:
            raise ViewDefinitionError(
                "A view has no dates: pass None for the business date and processing time, and the view will "
                "expose the milestoning columns instead")
        if self._order_by or self._limit is not None:
            raise ViewDefinitionError(
                "A view has no ORDER BY or row limit; order the rows when you read them with result().order_by()")
        projection = [Alias(column, _snake_case(display_alias(column))) for column in self._columns]
        view = ViewDefinition(name, schema, self._table, projection, self._op, self._group_by)
        check_view_joins(view)
        return view

    def to_sql(self) -> str:
        return to_sql(
            self._business_date, self._processing_datetime,
            self._columns, self._table, self._op,
            self._order_by, self._group_by, self._limit,
            business_date_to=self._business_date_to,
        )

    def to_pandas(self):
        return self._execute().to_pandas()

    def to_numpy(self):
        return self._execute().to_numpy()


def _snake_case(display_name: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]+', '_', display_name).strip('_').lower()


class ViewDefinition(View):
    """A relational View that can render itself as DDL and be read back through a ViewResult."""

    def to_sql(self, dialect: str | None = None) -> str:
        """``CREATE OR REPLACE VIEW`` DDL, in generic SQL or transpiled to ``dialect`` (e.g. 'databricks')."""
        return view_sql_to_string(self, dialect)

    def result(self) -> 'ViewResult':
        """The view's rows, read by creating it on the registered runner's backend."""
        return ViewResult(self)


class ViewResult(DataFrame):
    """The rows of a view.

    ``to_sql()`` is the view's CREATE VIEW DDL. ``to_pandas()`` and ``to_numpy()`` have the registered runner
    create the view, then select all of its rows. Like FinderResult, nothing runs until
    one of them is asked for.

    A view has no ORDER BY, so its rows come back in no particular order; ``order_by`` orders the read-back
    (not the view) by output column name.
    """

    def __init__(self, view: View):
        self._view = view
        self._order_by: list[str] = []
        self._timeout_ms: int = _DEFAULT_TIMEOUT_MS

    def order_by(self, *columns: str) -> 'ViewResult':
        unknown = [c for c in columns if c not in self._view.column_names()]
        if unknown:
            raise ValueError(f"Unknown view columns {unknown} — valid columns are: {self._view.column_names()}")
        self._order_by = list(columns)
        return self

    def timeout(self, milliseconds: int) -> 'ViewResult':
        self._timeout_ms = milliseconds
        return self

    def _execute(self) -> DataFrame:
        runner = QueryRunnerBase.get_runner()
        runner.create_view(self._view)
        return runner.select_view(self._view, self._order_by, self._timeout_ms)

    def to_sql(self, dialect: str | None = None) -> str:
        return view_sql_to_string(self._view, dialect)

    def to_pandas(self):
        return self._execute().to_pandas()

    def to_numpy(self):
        return self._execute().to_numpy()


def convert_inputs_and_select(business_date: Union[datetime.date, str] | None,
                              processing_datetime: Union[datetime.datetime, str] | None,
                              columns: list[Attribute], table: Table, op: Operation) -> FinderResult:
    bd = None if business_date is None else convert_date(business_date)
    pdt = None if processing_datetime is None else convert_date_time(processing_datetime)
    return FinderResult(bd, pdt, columns, table, op)


def convert_inputs_and_select_for_date_range(
        business_date_from: Union[datetime.date, str],
        business_date_to: Union[datetime.date, str],
        processing_datetime: Union[datetime.datetime, str],
        columns: list[Attribute], table: Table, op: Operation) -> FinderResult:
    if business_date_from is None:
        raise ValueError("business_date_from is required for find_for_date_range")
    if business_date_to is None:
        raise ValueError("business_date_to is required for find_for_date_range")
    if processing_datetime is None:
        raise ValueError("processing_valid_at is required for find_for_date_range")
    bd_from = convert_date(business_date_from)
    bd_to = convert_date(business_date_to)
    pdt = convert_date_time(processing_datetime)
    return FinderResult(bd_from, pdt, columns, table, op, business_date_to=bd_to)


class QueryRunnerBase(metaclass=RegistryBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime,
               columns: list[Attribute], table: Table, op: Operation,
               order_by: list[SortOperation] | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = _DEFAULT_TIMEOUT_MS,
               business_date_to: datetime.date | None = None) -> DataFrame:
        raise NotImplementedError()

    @staticmethod
    def create_view(view: View) -> None:
        """Create the view on the backend, replacing any existing view of that name."""
        raise NotImplementedError()

    @staticmethod
    def select_view(view: View, order_by: list[str] | None = None, timeout_ms: int = _DEFAULT_TIMEOUT_MS) -> DataFrame:
        """Return all rows of a view that already exists, ordered by the named view columns."""
        raise NotImplementedError()

    @staticmethod
    def get_runner():
        for k in RegistryBase.REGISTRY.keys():
            if k != 'QueryRunnerBase':
                return RegistryBase.REGISTRY[k]
        raise Exception("No query runner registered")

