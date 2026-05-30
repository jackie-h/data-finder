import datetime
import threading

from datafinder import Operation, DataFrame, Attribute, to_sql

import ibis
import numpy as np
import pandas as pd

from datafinder import QueryRunnerBase
from model.relational import Table


class IbisConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation, order_by: list = None, group_by: list = None,
               limit: int = None, timeout_ms: int = 60_000) -> DataFrame:
        conn = ibis.connect('duckdb://test.db')
        query = to_sql(business_date, processing_datetime, columns, table, op, order_by, group_by, limit)
        print(query)
        result: list = [None]
        error: list = [None]

        def run():
            try:
                result[0] = conn.sql(query)
            except Exception as e:
                error[0] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=timeout_ms / 1000)
        if thread.is_alive():
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")
        if error[0] is not None:
            raise error[0]
        return IbisOutput(result[0])


class IbisOutput(DataFrame):

    def __init__(self, t: ibis.Table):
        self.__table = t

    def to_numpy(self) -> np.array:
        return self.__table.__array__()

    def to_pandas(self) -> pd.DataFrame:
        return self.__table.to_pandas()
