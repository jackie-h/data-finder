import datetime
import itertools
import threading
from typing import Any, Sequence

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase, sql_for_dialect, register_dialect
from datafinder_spark.spark_sql import SparkFixes, spark_transforms
from datafinder.output import arrow_table_to_pandas, arrow_table_to_numpy, interrupt_after
from model.relational import Relation

import numpy as np
import pandas as pd
import pyarrow as pa
from sqlglot.dialects.spark import Spark
from sqlglot.generators.spark import SparkGenerator


class _Spark(Spark):
    """Spark as sqlglot writes it, changed where Spark 3.5 gives other rows than Postgres, DuckDB and the other engines (``spark_sql``)."""

    class Generator(SparkFixes, SparkGenerator):
        TRANSFORMS = spark_transforms(SparkGenerator.TRANSFORMS)


register_dialect("spark", _Spark)

_JOB_GROUPS = itertools.count(1)


def _to_spark_sql(business_date: datetime.date | None, processing_datetime: datetime.datetime | datetime.date | None,
                  columns: Sequence, table: Relation, op: Operation,
                  order_by: list | None = None, group_by: list | None = None,
                  limit: int | None = None, business_date_to: datetime.date | None = None,
                  distinct: bool = False, offset: int | None = None, having=None,
                  processing_datetime_to: datetime.datetime | datetime.date | None = None) -> str:
    generic_sql = to_sql(business_date, processing_datetime, columns, table, op,
                         order_by, group_by, limit, validate_sqlglot=False,
                         business_date_to=business_date_to, distinct=distinct, offset=offset, having=having,
                         processing_datetime_to=processing_datetime_to)
    return sql_for_dialect(generic_sql, 'spark')


_ARROW_TYPES = {
    "tinyint": pa.int8(), "smallint": pa.int16(), "int": pa.int32(), "bigint": pa.int64(), "float": pa.float32(), "double": pa.float64(),
    "string": pa.string(), "boolean": pa.bool_(), "date": pa.date32(), "timestamp_ntz": pa.timestamp("us"), "timestamp": pa.timestamp("us", tz="UTC"),
    "binary": pa.binary(),
}


def _arrow_schema(schema: Any) -> pa.Schema:
    """The Arrow schema of a Spark one, for a result that has no rows and so no batch to take it from."""
    def arrow_type(spark_type: Any) -> pa.DataType:
        name = spark_type.simpleString()
        if name.startswith("decimal("):
            precision, scale = (int(part) for part in name[len("decimal("):-1].split(","))
            return pa.decimal128(precision, scale)
        return _ARROW_TYPES.get(name, pa.string())
    return pa.schema([pa.field(field.name, arrow_type(field.dataType), nullable=field.nullable) for field in schema.fields])


def _arrow_table(frame: Any) -> pa.Table:
    """The rows of a Spark DataFrame as an Arrow Table with Spark's own types. PySpark 4 has ``toArrow``; PySpark 3.5, the last of the 3 line, has only
    the method that its ``toPandas`` is made with."""
    if hasattr(frame, "toArrow"):
        return frame.toArrow()
    batches = frame._collect_as_arrow()
    return pa.Table.from_batches(batches) if batches else _arrow_schema(frame.schema).empty_table()


class SparkConnect(QueryRunnerBase):
    """Runs the queries on a Spark session, which ``configure`` is given: a local one for the tests, or the one of a cluster. Like the other
    QueryRunnerBase implementations it is used via the class itself (``QueryRunnerBase.register(SparkConnect)``)."""

    _session: Any = None

    @classmethod
    def configure(cls, session: Any) -> None:
        """Set the ``pyspark.sql.SparkSession`` that select() runs its queries on."""
        cls._session = session

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime | datetime.date,
               columns: Sequence[Attribute], table: Relation, op: Operation,
               order_by: list | None = None, group_by: list | None = None,
               limit: int | None = None, timeout_ms: int = 60_000,
               business_date_to: datetime.date | None = None, distinct: bool = False,
               offset: int | None = None, having=None,
               processing_datetime_to: datetime.datetime | datetime.date | None = None) -> DataFrame:
        session = SparkConnect._session
        if session is None:
            raise RuntimeError("SparkConnect.configure(session) must be called before use")
        query = _to_spark_sql(business_date, processing_datetime, columns, table, op,
                              order_by, group_by, limit, business_date_to=business_date_to, distinct=distinct, offset=offset, having=having,
                              processing_datetime_to=processing_datetime_to)
        # The query is a job group of its own, so that a timeout can cancel the jobs that Spark is running for it and not only stop waiting.
        group = f"datafinder-{next(_JOB_GROUPS)}"
        context = session.sparkContext
        timed_out = threading.Event()

        def cancel() -> None:
            timed_out.set()
            context.cancelJobGroup(group)

        context.setJobGroup(group, query[:200], interruptOnCancel=True)
        try:
            with interrupt_after(timeout_ms, cancel):
                table_of_rows = _arrow_table(session.sql(query))
        except Exception as error:
            if timed_out.is_set():
                raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout") from error
            raise
        finally:
            for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
                context.setLocalProperty(key, None)  # the next query is not in this group
        return SparkOutput(table_of_rows)


class SparkOutput(DataFrame):
    """Wraps the rows of a query that Spark has run, as the PyArrow Table it gave, so that its own types are kept: to_pandas() and to_numpy() each
    convert it independently using Arrow-backed nullable dtypes, as the other engines' outputs do."""

    def __init__(self, table: pa.Table):
        self._table = table

    def to_pandas(self) -> pd.DataFrame:
        return arrow_table_to_pandas(self._table)

    def to_numpy(self) -> np.ndarray:
        return arrow_table_to_numpy(self._table)
