"""A TPC-BiH benchmark query, run two ways: as the report's SQL on DuckDB, and as a finder query.

A case holds the report's SQL with the values of its parameters, which of the result's columns are compared, and the expected
rows. The rows are generated (see tools/build_tpch_bih_cases.py) by running the SQL on the generated data, so they are what the
report's query returns. The SQL route checks that the SQL, as translated for DuckDB, still returns them; the finder route
checks that the finder query returns them too.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from datafinder_examples_tests.spec import _assert_rows_close
from datafinder_examples_tests.tpch_bih_sql import to_duckdb


def _text(value: Any) -> str:
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value.isoformat() if isinstance(value, datetime.date) else str(value)


@dataclass(frozen=True)
class BenchmarkCase:
    id: str
    #: the report's heading, for reading
    title: str
    #: the report's SQL, with its parameters written as [NAME]
    sql: str
    #: the value of each parameter: what the SQL and the finder query are both given
    parameters: dict[str, Any]
    #: the result columns that are compared, by name or position (for a column the SQL does not name). A finder cannot return
    #: every column, a foreign key for one, so this is the part of the result both routes can be compared on.
    select: tuple[str | int, ...]
    #: the SQL has an ORDER BY, so the rows are compared in order; otherwise they are sorted
    ordered: bool = False
    #: the compared columns are DISTINCT (the SQL repeats a row for versions the finder collapses)
    distinct: bool = False
    #: floating point values are compared to a relative 1e-9
    approximate: bool = False
    #: the expected result (generated)
    rows: tuple[tuple, ...] = field(default=(), compare=False)

    def statement(self) -> str:
        """The SQL with the parameters filled in."""
        sql = self.sql
        for name, value in self.parameters.items():
            sql = sql.replace(f"[{name}]", _text(value))
        return sql

    def project(self, names: list[str], rows: list[tuple]) -> list[tuple]:
        """The compared columns of a result, de-duplicated if ``distinct`` and sorted unless ``ordered``."""
        lowered = [n.lower() for n in names]
        positions = [c if isinstance(c, int) else lowered.index(c.lower()) for c in self.select]
        out = [tuple(row[i] for i in positions) for row in rows]
        if self.distinct:
            out = list(dict.fromkeys(out))
        if not self.ordered:
            out.sort(key=lambda row: tuple((v is None, v) for v in row))
        return out

    def run_sql(self, connection, schema: str | None = None) -> list[tuple]:
        """The report's SQL, translated for DuckDB and run on its tables."""
        cursor = connection.execute(to_duckdb(self.statement(), schema))
        names = [d[0] for d in cursor.description or []]
        return self.project(names, cursor.fetchall())

    def expected(self) -> np.ndarray:
        result = np.empty((len(self.rows), len(self.select)), dtype=object)
        for i, row in enumerate(self.rows):
            for j, value in enumerate(row):
                result[i, j] = value
        return result

    def check(self, actual: list[tuple]) -> None:
        """The rows the SQL returned are the expected ones."""
        result = np.empty((len(actual), len(self.select)), dtype=object)
        for i, row in enumerate(actual):
            for j, value in enumerate(row):
                result[i, j] = value
        if self.approximate:
            _assert_rows_close(result, self.expected(), self.id)
        else:
            assert actual == list(self.rows), f"[{self.id}] rows differ"
