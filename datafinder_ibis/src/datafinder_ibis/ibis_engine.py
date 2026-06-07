import datetime
import threading

from datafinder import Operation, DataFrame, Attribute, to_sql

import duckdb
import ibis
import numpy as np
import pandas as pd

from datafinder import QueryRunnerBase
from model.relational import Table


class IbisConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation, order_by: list = None, group_by: list = None,
               limit: int = None, timeout_ms: int = 60_000, business_date_to: datetime.date = None) -> DataFrame:
        conn = ibis.connect('duckdb://test.db')
        query = to_sql(business_date, processing_datetime, columns, table, op, order_by, group_by, limit, business_date_to=business_date_to)
        print(query)
        timer = threading.Timer(timeout_ms / 1000, conn.con.interrupt)
        timer.start()
        try:
            result = conn.sql(query)
        except duckdb.InterruptException:
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        finally:
            timer.cancel()
        return IbisOutput(result)


class IbisOutput(DataFrame):

    def __init__(self, t: ibis.Table):
        self.__table = t

    def to_numpy(self) -> np.array:
        return self.__table.__array__()

    def to_pandas(self) -> pd.DataFrame:
        return self.__table.to_pandas()
