import datetime

from datafinder import Operation, DataFrame, Attribute, select_sql_to_string, QueryRunnerBase, build_query_operation

import duckdb
import numpy as np
import pandas as pd

from model.relational import Table


class DuckDbConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation) -> DataFrame:
        conn = duckdb.connect('test.db')
        select_op = build_query_operation(business_date, processing_datetime, columns, table, op)
        query = select_sql_to_string(select_op)
        print(query)
        # TODO this is inefficient, could convert straight to desired output - such as numpy, instead of list
        return DuckDbOutput(conn.sql(query).fetchall())


class DuckDbOutput(DataFrame):
    __table: list

    def __init__(self, t: list):
        self.__table = t

    def to_numpy(self) -> np.array:
        #TODO - this could be a better dtype
        return np.array(self.__table, dtype='object')

    def to_pandas(self) -> pd.DataFrame:
        #todo - this needs to be better, to ensure types and column names
        return pd.DataFrame(self.__table)
