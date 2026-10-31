"""Compare what a spec's query returns with what Legend's own SQL returns on the same data."""
from __future__ import annotations

import datetime
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pytest
from sqlglot import exp

from datafinder_examples_tests.spec import TestExpectation
from datafinder_examples_tests.sql_compare import parse

from datafinder_legend_tests.legend import BACKEND, ROWS


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
    """The rows with each value in one canonical spelling, in the order given."""
    return [tuple(_canonical(v) for v in row) for row in rows]


def as_set_of_rows(rows: list[tuple]) -> list[tuple]:
    """The rows as an order-independent list."""
    return sorted(rows, key=repr)


def _sort_key_positions(legend_sql: str) -> list[int] | None:
    """Positions in the select list of Legend's ORDER BY expressions, or None if there is no ORDER BY or one of its
    expressions is not a selected column (so the order cannot be compared by column position)."""
    select = parse(legend_sql)
    order = select.args.get("order")
    if order is None:
        return None
    selected = [e.this if isinstance(e, exp.Alias) else e for e in select.expressions]
    aliases = [e.alias.lower() if isinstance(e, exp.Alias) else None for e in select.expressions]
    positions = []
    for ordered in order.expressions:
        key = ordered.this
        found = next((i for i, (e, alias) in enumerate(zip(selected, aliases))
                      if (alias is not None and isinstance(key, exp.Column) and not key.table and key.name.lower() == alias)
                      or e == key), None)
        if found is None:
            return None
        positions.append(found)
    return positions


def _for_duckdb(legend_sql: str) -> str:
    """Legend's SQL as DuckDB reads it: sqlglot maps functions such as char_length and ``select top N`` to DuckDB's,
    and a DateTime milestoning date, ``cast(truncate('2015-08-26 02:00:00') as date)``, becomes a cast of the text."""
    tree = parse(legend_sql)
    for cast in list(tree.find_all(exp.Cast)):
        call = cast.this
        if cast.to.this == exp.DataType.Type.DATE and (isinstance(call, exp.Trunc) or (
                isinstance(call, exp.Anonymous) and call.name.lower() == "truncate")):
            arg = call.this if isinstance(call, exp.Trunc) else call.expressions[0]
            cast.set("this", arg.copy())
    return tree.sql(dialect="duckdb")


def legend_rows(database: Any, expectation: TestExpectation) -> list[tuple]:
    """Legend's rows, in the order its SQL returns them."""
    conn = duckdb.connect(str(database), read_only=False)
    try:
        return canonical_rows(conn.execute(_for_duckdb(expectation.equivalent_sql[BACKEND])).fetchall())
    finally:
        conn.close()


def our_rows(finder: Any, expectation: TestExpectation) -> list[tuple]:
    """Our rows, in the order our query returns them."""
    df = expectation.query(finder).to_pandas()
    return canonical_rows(df.itertuples(index=False, name=None))


def assert_same_rows(database: Any, finder: Any, expectation: TestExpectation) -> None:
    """Our query returns the same rows as Legend's SQL does on the same data (columns by position). When Legend's SQL
    sorts, the rows must also come in the same order of the sorted columns."""
    if BACKEND in expectation.skip_backends:
        pytest.skip(expectation.skip_backends[BACKEND])
    gap = expectation.known_gaps.get(ROWS)
    if gap is not None:
        try:
            _assert_same_rows(database, finder, expectation)
        except AssertionError:
            pytest.xfail(gap)
        pytest.fail(f"[{expectation.name}] now returns Legend's rows: remove its {ROWS} known_gaps entry ({gap})")
    _assert_same_rows(database, finder, expectation)


def _assert_same_rows(database: Any, finder: Any, expectation: TestExpectation) -> None:
    legend_sql = expectation.equivalent_sql[BACKEND]
    expected = legend_rows(database, expectation)
    actual = our_rows(finder, expectation)
    context = (f"[{expectation.name}] rows differ from Legend's SQL on the same data.\n"
               f"Legend SQL: {legend_sql}\nLegend rows ({len(expected)}): {expected}\nOur rows ({len(actual)}): {actual}")
    assert as_set_of_rows(actual) == as_set_of_rows(expected), context
    keys = _sort_key_positions(legend_sql)
    if keys:
        assert [tuple(row[i] for i in keys) for row in actual] == [tuple(row[i] for i in keys) for row in expected], \
            context + f"\nThe rows are not in the same order of columns {keys}."
