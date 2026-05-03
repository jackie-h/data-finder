import datetime

from datafinder import Operation, DataFrame, Attribute, select_sql_to_string, build_query_operation

import ibis
import numpy as np
import pandas as pd

from datafinder import QueryRunnerBase
from model.relational import Table


class IbisConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation, order_by: list = None, group_by: list = None,
               limit: int = None) -> DataFrame:
        conn = ibis.connect('duckdb://test.db')
        select_op = build_query_operation(business_date, processing_datetime, columns, table, op, order_by or [], group_by or [], limit)
        query = select_sql_to_string(select_op)
        print(query)
        return IbisOutput(conn.sql(query))


class IbisOutput(DataFrame):

    def __init__(self, t: ibis.Table):
        self.__table = t

    def to_numpy(self) -> np.array:
        return self.__table.__array__()

    def to_pandas(self) -> pd.DataFrame:
        return self.__table.to_pandas()
