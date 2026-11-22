from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal


def _normalize_missing(values: np.ndarray) -> np.ndarray:
    """Normalize any missing-value marker (None, NaN, pd.NA, NaT, ...) to None.

    assert_array_equal raises TypeError on pd.NA ("boolean value of NA is ambiguous") even
    when both sides agree, since it isn't a plain Python/numpy scalar. Specs may reasonably
    write expected_result with None/np.nan while the actual nullable-dtype output uses
    pd.NA (or vice versa) — normalize both sides the same way so a real mismatch is still
    caught but the missing-value spelling doesn't matter.
    """
    flat = [None if pd.isna(v) else v for v in np.asarray(values, dtype=object).ravel()]
    return np.array(flat, dtype=object).reshape(np.asarray(values, dtype=object).shape)


def _assert_rows_close(actual: np.ndarray, expected: np.ndarray, name: str) -> None:
    """Rows equal, except that two floats are equal when they agree to a relative 1e-9: the last digits of an aggregate
    depend on the order the rows were added in, which is not defined."""
    assert actual.shape == expected.shape, f"[{name}] Result shape mismatch: {actual.shape} actual, {expected.shape} expected"
    for index in np.ndindex(actual.shape):
        a, e = actual[index], expected[index]
        if isinstance(a, float) and isinstance(e, float):
            assert math.isclose(a, e, rel_tol=1e-9, abs_tol=1e-9), f"[{name}] Result mismatch at {index}: {a!r} actual, {e!r} expected"
        else:
            assert a == e, f"[{name}] Result mismatch at {index}: {a!r} actual, {e!r} expected"


@dataclass
class Checks:
    """What a query must give: its columns and rows, and the SQL it writes. A spec written as files has them in its files (spec_files.checks)."""

    columns: list[str]
    rows: np.ndarray
    # The files of the full SQL a backend must write, keyed by backend name (e.g. "duckdb").
    sql_files: dict[str, Path] = field(default_factory=dict)
    # Keyed by backend name: a fragment the SQL must contain, and a fragment (e.g. "JOIN", to assert join elision) it must NOT contain.
    sql_contains: dict[str, str] = field(default_factory=dict)
    sql_not_contains: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name: the reason the expectation does not apply to that backend. The run is skipped.
    skip: dict[str, str] = field(default_factory=dict)
    # Compare floats to a relative 1e-9, not exactly: for an aggregate whose last digits depend on the order of the rows.
    approximate: bool = False


@dataclass
class TestExpectation:
    """A backend-agnostic query.

    A spec that is written as files (a suite of ``spec_files.SUITES``) is only the name and the query: its expected columns and rows
    are the CSV of the spec, its full SQL the ``.sql`` files, and its skips and tolerance the JSON, all in datafinder_specs. A spec that is
    not written as files carries them as the fields below.
    """

    name: str
    query: Callable  # (finder) -> FinderResult
    expected_columns: list[str] | None = None
    expected_result: np.ndarray | None = None
    # Keyed by backend name (e.g. "duckdb"). Omit to skip SQL check for that backend.
    expected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — fragment that must NOT appear (e.g. "JOIN", to assert join elision).
    unexpected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — reason the expectation does not apply to that backend. The run is skipped.
    skip_backends: dict[str, str] = field(default_factory=dict)
    # Compare floats to a relative 1e-9, not exactly: for an aggregate whose last digits depend on the order of the rows.
    approximate: bool = False
    # The type of each column, in the names of the model, when the values the spec expects do not say it (a column with no value).
    expected_types: list[str] | None = None

    def checks(self) -> Checks:
        if self.expected_columns is None or self.expected_result is None:
            from datafinder_examples_tests import spec_files
            return spec_files.checks(self)
        return Checks(self.expected_columns, self.expected_result, sql_contains=self.expected_sql, sql_not_contains=self.unexpected_sql,
                      skip=self.skip_backends, approximate=self.approximate)

    def run(self, finder: Any, backend: str | None = None) -> None:
        checks = self.checks()
        if backend and backend in checks.skip:
            pytest.skip(checks.skip[backend])
        result = self.query(finder)
        if backend and (backend in checks.sql_files or backend in checks.sql_contains or backend in checks.sql_not_contains):
            actual_sql = result.to_sql()
            if backend in checks.sql_files:
                self._assert_sql(checks.sql_files[backend], actual_sql)
            if backend in checks.sql_contains:
                fragment = checks.sql_contains[backend]
                assert fragment in actual_sql, (
                    f"[{self.name}] SQL fragment not found.\n"
                    f"Expected fragment: {fragment}\n"
                    f"Actual SQL: {actual_sql}"
                )
            if backend in checks.sql_not_contains:
                fragment = checks.sql_not_contains[backend]
                assert fragment not in actual_sql, (
                    f"[{self.name}] Unexpected SQL fragment found.\n"
                    f"Unwanted fragment: {fragment}\n"
                    f"Actual SQL: {actual_sql}"
                )
        df = result.to_pandas()
        assert list(df.columns) == checks.columns, (
            f"[{self.name}] Column mismatch.\n"
            f"Expected: {checks.columns}\n"
            f"Actual:   {list(df.columns)}"
        )
        if checks.approximate:
            _assert_rows_close(_normalize_missing(df.values), _normalize_missing(checks.rows), self.name)
            return
        assert_array_equal(
            _normalize_missing(df.values),
            _normalize_missing(checks.rows),
            err_msg=f"[{self.name}] Result mismatch",
        )

    def _assert_sql(self, expected_file: Path, actual_sql: str) -> None:
        """The SQL the engine writes is the file, whole. With ``DATAFINDER_UPDATE_SQL=1`` the file is written from the engine instead, to be read
        and committed like any change: it is a record of the SQL, so its result is checked by the CSV, never by the SQL."""
        if os.environ.get("DATAFINDER_UPDATE_SQL"):
            expected_file.write_text(actual_sql.strip() + "\n", encoding="utf-8")
            return
        assert expected_file.is_file(), f"[{self.name}] {expected_file} is missing: run with DATAFINDER_UPDATE_SQL=1 to write it"
        expected = expected_file.read_text(encoding="utf-8").strip()
        assert actual_sql.strip() == expected, (
            f"[{self.name}] SQL differs from {expected_file.name}.\n"
            f"Expected: {expected}\n"
            f"Actual SQL: {actual_sql.strip()}"
        )


@dataclass
class FinderSpec:
    """Groups expectations for a single finder class."""

    finder_name: str
    mapping_file: str
    expectations: list[TestExpectation]
