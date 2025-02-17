from datafinder import Operation, DataFrame, Attribute, select_sql_to_string

import ibis
import numpy as np
import pandas as pd

from datafinder import QueryRunnerBase


class IbisConnect(QueryRunnerBase):

    @staticmethod
    def select(columns: list[Attribute], table: str, op: Operation) -> DataFrame:
        conn = ibis.connect('duckdb://test.db')
        query = select_sql_to_string(columns, table, op)
        print(query)
        t = conn.table(table)
        #todo - can also do this with the dataframe API
        return IbisOutput(t.sql(query))


class IbisOutput(DataFrame):

    def __init__(self, t: ibis.Table):
        self.__table = t

    def to_numpy(self) -> np.array:
        return self.__table.__array__()

    def to_pandas(self) -> pd.DataFrame:
        return self.__table.to_pandas()
