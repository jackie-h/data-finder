from datafinder import Operation, QueryEngine, DataFrame, Attribute, SelectOperation

import duckdb
import numpy as np
import pandas as pd


class DuckDbConnect:

    @staticmethod
    def select(columns: list[Attribute], table: str, op: Operation) -> list:
        conn = duckdb.connect('test.db')
        qe = QueryEngine()
        select = SelectOperation(columns, table, op)
        select.generate_query(qe)
        query = qe.build_query_string()
        print(query)
        # TODO this is inefficient, could convert straight to desired output - such as numpy, instead of list
        return conn.sql(query).fetchall()


class DuckDbOutput(DataFrame):
    __table: list

    def __init__(self, t: list):
        self.__table = t

    def to_numpy(self) -> np.array:
        return np.array(self.__table)

    def to_pandas(self) -> pd.DataFrame:
        #todo - this needs to be better, to ensure types and column names
        return pd.DataFrame(self.__table)
