from datafinder import Operation, DataFrame, Attribute, select_sql_to_string

import ibis
import numpy as np
import pandas as pd


class IbisConnect:

    @staticmethod
    def select(columns: list[Attribute], table: str, op: Operation) -> ibis.Table:
        conn = ibis.connect('duckdb://test.db')
        query = select_sql_to_string(columns, table, op)
        print(query)
        t = conn.table(table)
        #todo - can also do this with the dataframe API
        return t.sql(query)


class IbisOutput(DataFrame):
    __table: ibis.Table

    def __init__(self, t: ibis.Table):
        self.__table = t

    def to_numpy(self) -> np.array:
        return np.array(self.__table)

    def to_pandas(self) -> pd.DataFrame:
        return self.__table.to_pandas()
