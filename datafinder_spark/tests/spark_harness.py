"""Loads the example data into Spark and gives the finder of a mapping that reads it, so that the shared specs run against Spark as they run against DuckDB."""
import contextlib
import importlib
import re
import shutil
import sys
import tempfile

import pytest

from datafinder import QueryRunnerBase
from datafinder_examples import example_path
from datafinder_examples_tests.tables import TableData
from datafinder_generator.generator import generate
from datafinder_spark.spark_engine import SparkConnect
from mapping_markdown.markdown_mapping import load


def _spark_columns(columns: str) -> str:
    """The columns as Spark's DDL spells them: a VARCHAR is a STRING there."""
    return re.sub(r"\bVARCHAR(\(\d+\))?", "STRING", columns, flags=re.IGNORECASE)


def load_tables(spark, tables: list[TableData]) -> None:
    """Each table is made afresh in its schema, from the rows of its CSV (none, for a table with none)."""
    for table in tables:
        schema = table.name.rpartition(".")[0]
        if schema:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        spark.sql(f"DROP TABLE IF EXISTS {table.name}")
        columns = _spark_columns(table.columns)
        if table.csv is None:
            spark.sql(f"CREATE TABLE {table.name} ({columns}) USING parquet")
        else:
            rows = spark.read.csv(str(example_path(table.csv)), header=True, schema=columns)
            rows.write.mode("overwrite").saveAsTable(table.name)


@contextlib.contextmanager
def finder_of(spark, mapping_file: str, tables: list[TableData], module: str, class_name: str):
    """The finder ``class_name`` that the mapping generates in ``module``, over Spark with the tables loaded."""
    SparkConnect.configure(spark)
    QueryRunnerBase.clear()
    QueryRunnerBase.register(SparkConnect)
    tmp = tempfile.mkdtemp()
    top = module.split(".")[0]
    cleared = lambda: [name for name in sys.modules if name == top or name.startswith(top + "_") or name.startswith(top + ".")]  # noqa: E731
    for name in cleared():
        sys.modules.pop(name, None)
    sys.path.insert(0, tmp)
    try:
        generate(load(str(example_path(mapping_file))), tmp)
        load_tables(spark, tables)
        yield getattr(importlib.import_module(module), class_name)()
    finally:
        if tmp in sys.path:
            sys.path.remove(tmp)
        for name in cleared():
            sys.modules.pop(name, None)
        shutil.rmtree(tmp, ignore_errors=True)


# Why a spec does not yet pass on Spark. Each is a strict expected failure, so one that starts to pass is reported and its entry removed.
LISTAGG = "Spark 3.5 has no LISTAGG (it has it from 4.0), which sqlglot writes STRING_AGG as: array_join(collect_list(x), sep) is the 3.5 form"
DISTINCT_ORDER = "Spark does not find the column of a SELECT DISTINCT by its table name in the ORDER BY: it is ordered by the output column"
ISODOW = "Spark has no EXTRACT(ISODOW …): the ISO day of the week is EXTRACT(DOW_ISO …)"
EPOCH = "Spark has no EXTRACT(EPOCH …): the seconds are UNIX_SECONDS(…)"
DATE_MINUS_DATE = "In Spark date - date is an interval, not the number of days: DATEDIFF(end, start) is"
DATE_PLUS_INTERVAL = "In Spark a date plus an interval is a date, where in DuckDB and Postgres it is a timestamp"


def expecting(expectations: list, gaps: dict[str, str]) -> list:
    """The expectations as test parameters, with those that do not pass on Spark yet (``gaps``, by name) as strict expected failures."""
    names = {each.name for each in expectations}
    stale = set(gaps) - names
    assert not stale, f"a gap is of a spec that is not there: {sorted(stale)}"
    return [pytest.param(each, id=each.name, marks=pytest.mark.xfail(reason=gaps[each.name], strict=True)) if each.name in gaps
            else pytest.param(each, id=each.name) for each in expectations]
