"""Loads the example data into Spark and gives the finder of a mapping that reads it, so that the shared specs run against Spark as they run against DuckDB."""
import contextlib
import importlib
import re
import shutil
import sys
import tempfile

from pyspark.sql.functions import col, from_json

from datafinder import QueryRunnerBase
from datafinder_examples import example_path
from datafinder_examples_tests.tables import TableData
from datafinder_generator.generator import generate
from datafinder_spark.spark_engine import SparkConnect
from mapping_markdown.markdown_mapping import load


_VECTOR = re.compile(r"\b(\w+)\s+VECTOR\(\d+\)", re.IGNORECASE)


def _spark_columns(columns: str) -> str:
    """The columns as Spark's DDL spells them: a VARCHAR is a STRING there, and a VECTOR is read as the text of its CSV, to be made an array."""
    return re.sub(r"\bVARCHAR(\(\d+\))?", "STRING", _VECTOR.sub(r"\1 STRING", columns), flags=re.IGNORECASE)


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
            for vector in _VECTOR.findall(table.columns):  # the text ``[0.5,0.25]`` is an array of floats
                rows = rows.withColumn(vector, from_json(col(vector), "array<float>"))
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

