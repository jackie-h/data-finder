import datetime
import threading

import sqlglot

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase
from datafinder.output import arrow_table_to_pandas, arrow_table_to_numpy
from model.relational import Table

import numpy as np
import pandas as pd
import pyarrow as pa


def _to_databricks_sql(business_date: datetime.date | None, processing_datetime: datetime.datetime | None,
                       columns: list, table: Table, op: Operation,
                       order_by: list | None = None, group_by: list | None = None,
                       limit: int | None = None, business_date_to: datetime.date | None = None) -> str:
    generic_sql = to_sql(business_date, processing_datetime, columns, table, op,
                         order_by, group_by, limit, validate_sqlglot=False,
                         business_date_to=business_date_to)
    return sqlglot.transpile(generic_sql, read='', write='databricks')[0]


class DatabricksConnect(QueryRunnerBase):
    """Like every other QueryRunnerBase implementation, this is used via the class itself
    (QueryRunnerBase.register(DatabricksConnect)), not an instance — the registry's __new__
    hook already auto-registers every subclass as a class, and get_runner() returns that
    class, calling .select() unbound. An instance-based __init__ holding credentials was
    never actually compatible with that: register(instance) would fail (RegistryBase.register
    reads clazz.__name__, which an instance doesn't have), and select() would receive
    business_date positionally in self's slot. Call configure() once before use instead.
    """

    _server_hostname: str | None = None
    _http_path: str | None = None
    _access_token: str | None = None

    @classmethod
    def configure(cls, server_hostname: str, http_path: str, access_token: str) -> None:
        """Set the Databricks connection credentials used by select()."""
        cls._server_hostname = server_hostname
        cls._http_path = http_path
        cls._access_token = access_token

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime,
               columns: list[Attribute], table: Table, op: Operation,
               order_by: list | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = 60_000,
               business_date_to: datetime.date | None = None) -> DataFrame:
        if DatabricksConnect._server_hostname is None:
            raise RuntimeError("DatabricksConnect.configure(...) must be called before use")
        query = _to_databricks_sql(business_date, processing_datetime, columns, table, op,
                                   order_by, group_by, limit, business_date_to=business_date_to)
        result: list = [None]
        error: list = [None]

        def run():
            try:
                from databricks import sql as databricks_sql
                with databricks_sql.connect(
                    server_hostname=DatabricksConnect._server_hostname,
                    http_path=DatabricksConnect._http_path,
                    access_token=DatabricksConnect._access_token,
                ) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        # PyArrow carries the query's own schema (from the SQL aliases and
                        # Databricks' native column types), rather than an untyped list of
                        # row tuples pandas would have to guess dtypes for.
                        result[0] = cursor.fetchall_arrow()
            except Exception as e:
                error[0] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=timeout_ms / 1000)
        if thread.is_alive():
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        if error[0] is not None:
            raise error[0]
        return DatabricksOutput(result[0])


class DatabricksOutput(DataFrame):
    """Wraps an already-fetched PyArrow Table (Databricks' cursor has no lazy/unmaterialized
    query handle to defer to, unlike DuckDB/Ibis — the round trip already happened in
    select()). to_pandas() and to_numpy() each convert it independently using Arrow-backed
    nullable dtypes, so to_numpy() still never allocates a DataFrame it doesn't need."""

    def __init__(self, table: pa.Table):
        self._table = table

    def to_pandas(self) -> pd.DataFrame:
        return arrow_table_to_pandas(self._table)

    def to_numpy(self) -> np.ndarray:
        return arrow_table_to_numpy(self._table)
