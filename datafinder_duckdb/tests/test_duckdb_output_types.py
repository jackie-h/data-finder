"""Verify DuckDbOutput.to_pandas()/to_numpy() carry correct column names and correct, nullable
per-column types (Arrow-backed dtypes), rather than an unnamed frame with pandas' best-guess
dtypes from raw row tuples."""
import datetime

import duckdb
import numpy as np
import pandas as pd
import pytest

from datafinder import QueryRunnerBase, StringAttribute, IntegerAttribute, DoubleAttribute, BooleanAttribute
from datafinder_duckdb.duckdb_engine import DuckDbConnect
from model.relational import Table, NoOperation

_BUSINESS_DATE = datetime.date(2024, 1, 1)
_PROCESSING_DATETIME = datetime.datetime(2024, 1, 1, 0, 0, 0)


@pytest.fixture
def con():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS type_check CASCADE")
    conn.execute("CREATE SCHEMA type_check")
    conn.execute(
        "CREATE TABLE type_check.widgets (name VARCHAR, quantity INTEGER, price DOUBLE, active BOOLEAN)"
    )
    conn.execute("INSERT INTO type_check.widgets VALUES ('Widget', 10, 4.5, true)")
    conn.close()
    QueryRunnerBase.clear()
    QueryRunnerBase.register(DuckDbConnect)
    yield
    conn2 = duckdb.connect("test.db")
    conn2.execute("DROP SCHEMA IF EXISTS type_check CASCADE")
    conn2.close()


def _columns():
    table = Table("type_check.widgets", [])
    name_attr = StringAttribute("Name", "name", "VARCHAR", "type_check.widgets")
    qty_attr = IntegerAttribute("Quantity", "quantity", "INTEGER", "type_check.widgets")
    price_attr = DoubleAttribute("Price", "price", "DOUBLE", "type_check.widgets")
    active_attr = BooleanAttribute("Active", "active", "BOOLEAN", "type_check.widgets")
    return table, [name_attr, qty_attr, price_attr, active_attr]


class TestDuckDbOutputTypes:

    def test_pandas_column_names_match_display_names(self, con):
        table, columns = _columns()
        df = DuckDbConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation()).to_pandas()
        assert list(df.columns) == ["Name", "Quantity", "Price", "Active"]

    def test_pandas_dtypes_are_nullable_and_correct(self, con):
        table, columns = _columns()
        df = DuckDbConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation()).to_pandas()
        assert str(df["Name"].dtype) == "string[pyarrow]"
        assert str(df["Quantity"].dtype) == "int32[pyarrow]"
        assert str(df["Price"].dtype) == "double[pyarrow]"
        assert str(df["Active"].dtype) == "bool[pyarrow]"

    def test_numpy_cell_types_match_model_types(self, con):
        table, columns = _columns()
        row = DuckDbConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation()).to_numpy()[0]
        name, quantity, price, active = row
        assert isinstance(name, str) and name == "Widget"
        assert isinstance(quantity, (int, np.integer)) and quantity == 10
        assert isinstance(price, (float, np.floating)) and price == 4.5
        assert isinstance(active, (bool, np.bool_)) and active is True

    def test_null_value_is_pd_na_in_both_pandas_and_numpy(self, con):
        conn = duckdb.connect("test.db")
        conn.execute("INSERT INTO type_check.widgets VALUES (NULL, NULL, NULL, NULL)")
        conn.close()
        table, columns = _columns()
        output = DuckDbConnect.select(_BUSINESS_DATE, _PROCESSING_DATETIME, columns, table, NoOperation())
        df = output.to_pandas()
        assert df["Quantity"].iloc[1] is pd.NA
        arr = output.to_numpy()
        assert arr[1][1] is pd.NA
