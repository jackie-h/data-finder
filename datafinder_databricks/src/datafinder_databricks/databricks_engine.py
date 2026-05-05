import datetime

import sqlglot

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase
from model.relational import Table

import numpy as np
import pandas as pd


def _to_databricks_sql(business_date: datetime.date, processing_datetime: datetime.datetime,
                       columns: list, table: Table, op: Operation,
                       order_by: list = None, group_by: list = None,
                       limit: int = None) -> str:
    generic_sql = to_sql(business_date, processing_datetime, columns, table, op,
                         order_by, group_by, limit, validate_sqlglot=False)
    return sqlglot.transpile(generic_sql, read='', write='databricks')[0]


class DatabricksConnect(QueryRunnerBase):

    def __init__(self, server_hostname: str, http_path: str, access_token: str):
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token

    def select(self, business_date: datetime.date, processing_datetime: datetime.datetime,
               columns: list[Attribute], table: Table, op: Operation,
               order_by: list = None, group_by: list = None,
               limit: int = None) -> DataFrame:
        query = _to_databricks_sql(business_date, processing_datetime, columns, table, op,
                                   order_by, group_by, limit)
        from databricks import sql as databricks_sql
        with databricks_sql.connect(
            server_hostname=self._server_hostname,
            http_path=self._http_path,
            access_token=self._access_token,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns_meta = [desc[0] for desc in cursor.description]
        return DatabricksOutput(rows, columns_meta)


class DatabricksOutput(DataFrame):

    def __init__(self, rows: list, columns: list[str]):
        self._rows = rows
        self._columns = columns

    def to_numpy(self) -> np.ndarray:
        return np.array(self._rows, dtype='object')

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self._rows, columns=self._columns)
