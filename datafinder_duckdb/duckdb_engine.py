from datafinder import Operation, DataFrame, Attribute, select_sql_to_string

import duckdb
import numpy as np
import pandas as pd


class DuckDbConnect:

    @staticmethod
    def select(columns: list[Attribute], table: str, op: Operation) -> list:
        conn = duckdb.connect('test.db')
        query = select_sql_to_string(columns, table, op)
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
