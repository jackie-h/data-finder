import datetime
import threading

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase

import duckdb
import numpy as np
import pandas as pd

from model.relational import Table


class DuckDbConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation, order_by: list | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = 60_000, business_date_to: datetime.date | None = None) -> DataFrame:
        conn = duckdb.connect('test.db')
        query = to_sql(business_date, processing_datetime, columns, table, op, order_by, group_by, limit, business_date_to=business_date_to)
        print(query)
        timer = threading.Timer(timeout_ms / 1000, conn.interrupt)
        timer.start()
        try:
            # .df() carries the query's own column names (from the SQL aliases) and DuckDB's
            # native per-column types, rather than an untyped, unnamed list of row tuples.
            result = conn.sql(query).df()
        except duckdb.InterruptException:
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        finally:
            timer.cancel()
        return DuckDbOutput(result)


class DuckDbOutput(DataFrame):
    __df: pd.DataFrame

    def __init__(self, df: pd.DataFrame):
        self.__df = df

    def to_numpy(self) -> np.ndarray:
        return self.__df.to_numpy()

    def to_pandas(self) -> pd.DataFrame:
        return self.__df.copy()
