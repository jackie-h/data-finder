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
        # conn.sql() only builds the query plan — nothing executes (and no pandas DataFrame
        # gets built) until DuckDbOutput actually materializes it, driven by which of
        # to_pandas()/to_numpy() the caller asks for.
        relation = conn.sql(query)
        return DuckDbOutput(conn, relation, timeout_ms)


class DuckDbOutput(DataFrame):
    """Wraps an unmaterialized DuckDB relation. A relation can be (re-)materialized any number
    of times, so to_pandas() and to_numpy() each only do the work — and only pay for a pandas
    DataFrame — their own conversion actually needs."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, relation: duckdb.DuckDBPyRelation, timeout_ms: int):
        self.__conn = conn
        self.__relation = relation
        self.__timeout_ms = timeout_ms
        self.__df: pd.DataFrame | None = None

    def __materialize(self, fetch):
        timer = threading.Timer(self.__timeout_ms / 1000, self.__conn.interrupt)
        timer.start()
        try:
            return fetch()
        except duckdb.InterruptException:
            raise TimeoutError(f"Query exceeded {self.__timeout_ms}ms timeout")
        finally:
            timer.cancel()

    def to_pandas(self) -> pd.DataFrame:
        if self.__df is None:
            # .df() carries the query's own column names (from the SQL aliases) and DuckDB's
            # native per-column types, rather than an untyped, unnamed list of row tuples.
            self.__df = self.__materialize(self.__relation.df)
        return self.__df.copy()

    def to_numpy(self) -> np.ndarray:
        if self.__df is not None:
            return self.__df.to_numpy()
        columns = self.__materialize(self.__relation.fetchnumpy)
        arrays = []
        for arr in columns.values():
            if isinstance(arr, np.ma.MaskedArray):
                filled = arr.astype(object)
                filled[arr.mask] = None
                arrays.append(filled)
            else:
                arrays.append(np.asarray(arr, dtype=object))
        if not arrays or len(arrays[0]) == 0:
            return np.empty((0, len(arrays)), dtype=object)
        return np.column_stack(arrays)
