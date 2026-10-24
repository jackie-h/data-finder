"""Compare what a spec's query returns with what Legend's own SQL returns on the same data."""
from __future__ import annotations

import datetime
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pytest

from datafinder_examples_tests.spec import TestExpectation
from datafinder_examples_tests.sql_compare import parse

from datafinder_legend_tests.legend import BACKEND


def _canonical(value: Any) -> Any:
    """One spelling per value, so rows from different engines compare: missing is None, dates are datetimes."""
    if value is None or value is pd.NaT or (not isinstance(value, (str, bytes)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.time.min)
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, np.generic):  # numpy scalar
        return _canonical(value.item())
    return value


def canonical_rows(rows) -> list[tuple]:
    """The rows as an order-independent list: the specs compare sets of rows, not their order."""
    return sorted((tuple(_canonical(v) for v in row) for row in rows), key=repr)


def _for_duckdb(legend_sql: str) -> str:
    """Legend's SQL as DuckDB reads it: sqlglot maps functions such as char_length and ``select top N`` to DuckDB's."""
    return parse(legend_sql).sql(dialect="duckdb")


def legend_rows(database: Any, expectation: TestExpectation) -> list[tuple]:
    conn = duckdb.connect(str(database), read_only=False)
    try:
        return canonical_rows(conn.execute(_for_duckdb(expectation.equivalent_sql[BACKEND])).fetchall())
    finally:
        conn.close()


def our_rows(finder: Any, expectation: TestExpectation) -> list[tuple]:
    df = expectation.query(finder).to_pandas()
    return canonical_rows(df.itertuples(index=False, name=None))


def assert_same_rows(database: Any, finder: Any, expectation: TestExpectation) -> None:
    """Our query returns the same rows as Legend's SQL does on the same data (columns by position)."""
    if BACKEND in expectation.skip_backends:
        pytest.skip(expectation.skip_backends[BACKEND])
    expected = legend_rows(database, expectation)
    actual = our_rows(finder, expectation)
    assert actual == expected, (
        f"[{expectation.name}] rows differ from Legend's SQL on the same data.\n"
        f"Legend SQL: {expectation.equivalent_sql[BACKEND]}\n"
        f"Legend rows ({len(expected)}): {expected}\nOur rows ({len(actual)}): {actual}"
    )
