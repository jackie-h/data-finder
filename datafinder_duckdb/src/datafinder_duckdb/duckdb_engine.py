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
            result = conn.sql(query).fetchall()
        except duckdb.InterruptException:
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        finally:
            timer.cancel()
        return DuckDbOutput(result)


class DuckDbOutput(DataFrame):
    __table: list

    def __init__(self, t: list):
        self.__table = t

    def to_numpy(self) -> np.ndarray:
        #TODO - this could be a better dtype
        return np.array(self.__table, dtype='object')

    def to_pandas(self) -> pd.DataFrame:
        #todo - this needs to be better, to ensure types and column names
        return pd.DataFrame(self.__table)
