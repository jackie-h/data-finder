import datetime
import itertools
import threading
from typing import Any, Sequence

from datafinder import Operation, DataFrame, Attribute, to_sql, QueryRunnerBase, sql_for_dialect, register_dialect
from datafinder.output import arrow_table_to_pandas, arrow_table_to_numpy, interrupt_after
from model.relational import Relation

import numpy as np
import sqlglot
import pandas as pd
import pyarrow as pa
from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.generator import Generator
from sqlglot.generators.spark import SparkGenerator


def _unwrapped(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _is_cast_to(node: exp.Expr, type_: Any) -> bool:
    return isinstance(node, exp.Cast) and node.to.this == type_


def _joined(aggregate: exp.GroupConcat) -> exp.Expr:
    """``STRING_AGG(x, sep ORDER BY k ...)`` as Spark 3.5 has it, which has no LISTAGG (4.0 has): the values are collected, sorted by the keys with
    a comparator, and joined. A null value is left out, as STRING_AGG leaves it, and a null key is ordered as the sort says."""
    values = aggregate.this
    keys: list[exp.Ordered] = []
    if isinstance(values, exp.Order):
        keys, values = list(values.expressions), values.this
    separator = aggregate.args.get("separator") or exp.Literal.string("")
    if not keys:
        return exp.func("ARRAY_JOIN", exp.func("COLLECT_LIST", values), separator)
    fields = ", ".join(["__value AS v"] + [f"__key{i} AS k{i}" for i in range(len(keys))])
    comparisons = []
    for i, key in enumerate(keys):
        before, after = ("-1", "1") if not key.args.get("desc") else ("1", "-1")
        first, last = ("-1", "1") if key.args.get("nulls_first") else ("1", "-1")
        l, r = f"l.k{i}", f"r.k{i}"
        comparisons.append(f"NULLIF(CASE WHEN {l} IS NULL AND {r} IS NULL THEN 0 WHEN {l} IS NULL THEN {first} WHEN {r} IS NULL THEN {last} "
                           f"WHEN {l} < {r} THEN {before} WHEN {l} > {r} THEN {after} ELSE 0 END, 0)")
    template = sqlglot.parse_one(
        f"ARRAY_JOIN(TRANSFORM(ARRAY_SORT(COLLECT_LIST(CASE WHEN __value IS NOT NULL THEN STRUCT({fields}) END), "
        f"(l, r) -> COALESCE({', '.join(comparisons)}, 0)), s -> s.v), __separator)", dialect="spark")
    substitutes: dict[str, exp.Expr] = {"__value": values, "__separator": separator, **{f"__key{i}": key.this for i, key in enumerate(keys)}}
    for column in list(template.find_all(exp.Column)):
        if column.name in substitutes:
            column.replace(substitutes[column.name].copy())
    return template


def _for_spark(tree: exp.Expr) -> exp.Expr:
    """The Postgres tree changed where Spark 3.5 differs, so that the same query gives the same rows."""
    for node in list(tree.find_all(exp.Extract)):
        part = node.this.name.upper()
        difference = _unwrapped(node.expression)
        if part == "ISODOW":  # Spark spells the ISO day of the week DOW_ISO
            node.set("this", exp.var("DOW_ISO"))
        elif part == "EPOCH" and isinstance(difference, exp.Sub):  # the seconds between two timestamps, which Spark gives as an interval
            node.replace(exp.Paren(this=exp.Sub(this=exp.func("UNIX_SECONDS", difference.this.copy()),
                                                expression=exp.func("UNIX_SECONDS", difference.expression.copy()))))
    for node in list(tree.find_all(exp.Sub, exp.Add)):
        if isinstance(node, exp.Sub) and _is_cast_to(node.this, exp.DataType.Type.DATE) and _is_cast_to(node.expression, exp.DataType.Type.DATE):
            # date - date is an interval in Spark, and the number of days in Postgres
            node.replace(exp.func("DATEDIFF", node.this.copy(), node.expression.copy()))
        elif isinstance(node.expression, exp.Interval) and not _is_cast_to(node.this, exp.DataType.Type.TIMESTAMP):
            # date +/- interval is a timestamp in Postgres, and a date in Spark
            node.this.replace(exp.cast(node.this.copy(), exp.DataType.Type.TIMESTAMP))
    for select in list(tree.find_all(exp.Select)):
        order = select.args.get("order")
        if select.args.get("distinct") and order:
            # a SELECT DISTINCT is ordered by what it selects: Spark does not find a column of the table in the ORDER BY once it is aliased
            shown = {each.this.sql(): each.args["alias"] for each in select.expressions if isinstance(each, exp.Alias)}
            for sort in order.expressions:
                alias = shown.get(sort.this.sql())
                if alias is not None:
                    sort.set("this", exp.column(alias.copy()))
    for aggregate in list(tree.find_all(exp.GroupConcat)):
        if not isinstance(aggregate.parent, exp.Window):
            aggregate.replace(_joined(aggregate))
    return tree


class _Spark(Spark):
    """Spark as sqlglot writes it, but with a percentile left as ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)``, which Spark has (from 3.3)
    and which is exact; sqlglot's Spark writes it as ``PERCENTILE_APPROX(x, p)``, an approximation that has no order. The query it is given is
    changed first (``_for_spark``) where Spark 3.5 does what Postgres does not."""

    class Generator(SparkGenerator):
        TRANSFORMS = {**SparkGenerator.TRANSFORMS, exp.WithinGroup: lambda self, e: Generator.withingroup_sql(self, e)}

        def preprocess(self, expression: exp.Expr) -> exp.Expr:
            return super().preprocess(_for_spark(expression))


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
