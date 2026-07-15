import datetime
import threading

from datafinder import Operation, DataFrame, Attribute, to_sql
from datafinder.output import arrow_table_to_pandas, arrow_table_to_numpy

import duckdb
import ibis
import numpy as np
import pandas as pd
import pyarrow as pa

from datafinder import QueryRunnerBase
from model.relational import Table


class IbisConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime, columns: list[Attribute],
               table: Table, op: Operation, order_by: list | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = 60_000, business_date_to: datetime.date | None = None) -> DataFrame:
        conn = ibis.connect('duckdb://test.db')
        query = to_sql(business_date, processing_datetime, columns, table, op, order_by, group_by, limit, business_date_to=business_date_to)
        print(query)
        # conn.sql() only builds the expression — nothing executes until IbisOutput actually
        # materializes it, driven by which of to_pandas()/to_numpy() the caller asks for.
        expr = conn.sql(query)  # type: ignore[attr-defined]
        return IbisOutput(conn, expr, timeout_ms)


class IbisOutput(DataFrame):
    """Wraps an unmaterialized Ibis expression. Materializes at most once, into a cached
    PyArrow Table (not a pandas DataFrame) — to_pandas() and to_numpy() each convert that
    independently, using Arrow-backed nullable dtypes so a NULL doesn't silently upcast e.g.
    an integer column to float64, without to_numpy() ever allocating a DataFrame it doesn't
    need."""

    def __init__(self, conn: 'ibis.BaseBackend', expr: 'ibis.Table', timeout_ms: int):
        self.__conn = conn
        self.__expr = expr
        self.__timeout_ms = timeout_ms
        self.__arrow: pa.Table | None = None

    def __materialize(self) -> pa.Table:
        arrow = self.__arrow
        if arrow is None:
            timer = threading.Timer(self.__timeout_ms / 1000, self.__conn.con.interrupt)  # type: ignore[attr-defined]
            timer.start()
            try:
                arrow = self.__expr.to_pyarrow()
            except duckdb.InterruptException:
                raise TimeoutError(f"Query exceeded {self.__timeout_ms}ms timeout")
            finally:
                timer.cancel()
            self.__arrow = arrow
        return arrow

    def to_pandas(self) -> pd.DataFrame:
        return arrow_table_to_pandas(self.__materialize())

    def to_numpy(self) -> np.ndarray:
        return arrow_table_to_numpy(self.__materialize())
