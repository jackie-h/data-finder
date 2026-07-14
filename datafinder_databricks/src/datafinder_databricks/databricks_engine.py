import datetime
import threading

import sqlglot

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase
from datafinder.output import build_typed_dataframe
from model.relational import Table

import numpy as np
import pandas as pd


def _to_databricks_sql(business_date: datetime.date | None, processing_datetime: datetime.datetime | None,
                       columns: list, table: Table, op: Operation,
                       order_by: list | None = None, group_by: list | None = None,
                       limit: int | None = None, business_date_to: datetime.date | None = None) -> str:
    generic_sql = to_sql(business_date, processing_datetime, columns, table, op,
                         order_by, group_by, limit, validate_sqlglot=False,
                         business_date_to=business_date_to)
    return sqlglot.transpile(generic_sql, read='', write='databricks')[0]


class DatabricksConnect(QueryRunnerBase):

    def __init__(self, server_hostname: str, http_path: str, access_token: str):
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token

    def select(self, business_date: datetime.date, processing_datetime: datetime.datetime,  # type: ignore[override]
               columns: list[Attribute], table: Table, op: Operation,
               order_by: list | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = 60_000,
               business_date_to: datetime.date | None = None) -> DataFrame:
        query = _to_databricks_sql(business_date, processing_datetime, columns, table, op,
                                   order_by, group_by, limit, business_date_to=business_date_to)
        result: list = [None]
        error: list = [None]

        def run():
            try:
                from databricks import sql as databricks_sql
                with databricks_sql.connect(
                    server_hostname=self._server_hostname,
                    http_path=self._http_path,
                    access_token=self._access_token,
                ) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        result[0] = (cursor.fetchall(), [desc[0] for desc in (cursor.description or [])])
            except Exception as e:
                error[0] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=timeout_ms / 1000)
        if thread.is_alive():
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        if error[0] is not None:
            raise error[0]
        rows, columns_meta = result[0]
        return DatabricksOutput(rows, columns_meta, columns)


class DatabricksOutput(DataFrame):

    def __init__(self, rows: list, columns: list[str], display_columns: list | None = None):
        self._rows = rows
        self._columns = columns
        self._display_columns = display_columns

    def to_numpy(self) -> np.ndarray:
        return self.to_pandas().to_numpy()

    def to_pandas(self) -> pd.DataFrame:
        return build_typed_dataframe(self._rows, self._columns, self._display_columns)
