from datafinder import Operation, QueryEngine, DataFrame

import ibis
import numpy as np
import pandas as pd


class IbisConnect:

    @staticmethod
    def select(table: str, op: Operation, columns: list[str]) -> ibis.Table:
        conn = ibis.connect('duckdb://test.db')
        qe = QueryEngine()
        op.generate_query(qe)
        query = "select * from " + table + " where " + qe.build_query_string()
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
