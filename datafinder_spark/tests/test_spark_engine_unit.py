"""What the Spark engine does that no finder spec shows: it needs a session, it cancels a query that times out, it gives the types Spark gave, and it
writes a percentile as the exact one. What a query gives is in the shared specs, which test_specs_*.py run against Spark."""
import time

import pytest

from datafinder import StringAttribute, sql_for_dialect
from datafinder_spark import spark_engine
from datafinder_spark.spark_engine import SparkConnect, _arrow_table
from model.relational import NoOperation, Table

_OWNER = Table("t", [])


def test_select_needs_a_session(monkeypatch):
    monkeypatch.setattr(SparkConnect, "_session", None)
    with pytest.raises(RuntimeError, match="configure"):
        SparkConnect.select(None, None, [StringAttribute("Name", "NAME", "VARCHAR", _OWNER)], _OWNER, NoOperation())  # pyright: ignore[reportArgumentType]


def test_a_query_that_times_out_is_cancelled_on_spark(spark, monkeypatch):
    spark.udf.register("slow_down", lambda value: (time.sleep(30), value)[1])
    monkeypatch.setattr(SparkConnect, "_session", spark)
    monkeypatch.setattr(spark_engine, "_to_spark_sql", lambda *args, **kwargs: "SELECT slow_down(id) FROM range(2)")
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="500ms"):
        SparkConnect.select(None, None, [], _OWNER, NoOperation(), timeout_ms=500)  # pyright: ignore[reportArgumentType]
    assert time.monotonic() - started < 20  # not the 30 seconds that the query would take
    assert [tuple(row) for row in spark.sql("SELECT 1").collect()] == [(1,)]  # the session is as it was: the next query is not cancelled


def test_the_rows_are_an_arrow_table_of_the_types_spark_gave(spark):
    table = _arrow_table(spark.sql("SELECT CAST(NULL AS INT) AS a, 2L AS b, TIMESTAMP '2024-03-10 08:00:00' AS t, DATE '2024-03-10' AS d, "
                                   "CAST(3 AS DECIMAL(10,2)) AS dec, 'z' AS s"))
    assert [str(field.type) for field in table.schema] == ["int32", "int64", "timestamp[us]", "date32[day]", "decimal128(10, 2)", "string"]
    assert table.to_pylist()[0]["a"] is None  # a missing integer is missing, and not a float NaN


def test_a_result_with_no_rows_has_the_types_of_its_columns(spark):
    table = _arrow_table(spark.sql("SELECT 2L AS b, 'z' AS s, DATE '2024-03-10' AS d WHERE 1 = 0"))
    assert table.num_rows == 0 and [str(field.type) for field in table.schema] == ["int64", "string", "date32[day]"]


def test_a_percentile_is_written_as_the_exact_one_and_not_the_approximation():
    sql = sql_for_dialect("SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY x DESC) FROM t", "spark")
    assert "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY x DESC" in sql and "PERCENTILE_APPROX" not in sql
