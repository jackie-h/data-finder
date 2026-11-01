from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal

from datafinder_examples_tests.sql_compare import normalize_sql, sql_equivalent


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


@dataclass
class TestExpectation:
    """A single backend-agnostic query expectation."""

    name: str
    query: Callable  # (query_context) -> FinderResult
    # Omit both for a SQL-only expectation: the query is built and its SQL checked, but not executed.
    expected_columns: list[str] | None = None
    expected_result: np.ndarray | None = None
    # Keyed by backend name (e.g. "duckdb"). Omit to skip SQL check for that backend.
    expected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — SQL that must be structurally equivalent to the actual SQL: table aliases, output
    # column names, identifier quoting/case and the default schema are ignored (see sql_compare).
    equivalent_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — a real difference from the expectation that is understood but not yet fixed. The
    # checks still run: they are expected to fail (reported as xfail) and a pass is an error, so the entry is
    # removed as soon as the gap is closed.
    known_gaps: dict[str, str] = field(default_factory=dict)
    # Where the expectation came from, e.g. the legend-engine test file and function it was ported from.
    source: str = ""
    # Keyed by backend name — fragment that must NOT appear (e.g. "JOIN", to assert join elision).
    unexpected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — reason the expectation does not apply to that backend. The run is skipped.
    skip_backends: dict[str, str] = field(default_factory=dict)

    def run(self, query_context: Any, backend: str | None = None) -> None:
        if backend and backend in self.skip_backends:
            pytest.skip(self.skip_backends[backend])
        if backend and backend in self.known_gaps:
            try:
                self._check(query_context, backend)
            except Exception:
                pytest.xfail(self.known_gaps[backend])
            pytest.fail(f"[{self.name}] now passes: remove its known_gaps entry ({self.known_gaps[backend]})")
        self._check(query_context, backend)

    def _check(self, query_context: Any, backend: str | None) -> None:
        result = self.query(query_context)
        if backend and (backend in self.expected_sql or backend in self.unexpected_sql
                        or backend in self.equivalent_sql):
            actual_sql = result.to_sql()
            if backend in self.equivalent_sql:
                expected = self.equivalent_sql[backend]
                assert sql_equivalent(actual_sql, expected), (
                    f"[{self.name}] SQL not equivalent.\n"
                    f"Expected: {expected}\n  normalized: {normalize_sql(expected)}\n"
                    f"Actual:   {actual_sql}\n  normalized: {normalize_sql(actual_sql)}"
                )
            if backend in self.expected_sql:
                fragment = self.expected_sql[backend]
                assert fragment in actual_sql, (
                    f"[{self.name}] SQL fragment not found.\n"
                    f"Expected fragment: {fragment}\n"
                    f"Actual SQL: {actual_sql}"
                )
            if backend in self.unexpected_sql:
                fragment = self.unexpected_sql[backend]
                assert fragment not in actual_sql, (
                    f"[{self.name}] Unexpected SQL fragment found.\n"
                    f"Unwanted fragment: {fragment}\n"
                    f"Actual SQL: {actual_sql}"
                )
        if self.expected_result is None:
            return
        df = result.to_pandas()
        assert list(df.columns) == self.expected_columns, (
            f"[{self.name}] Column mismatch.\n"
            f"Expected: {self.expected_columns}\n"
            f"Actual:   {list(df.columns)}"
        )
        assert_array_equal(
            _normalize_missing(df.values),
            _normalize_missing(self.expected_result),
            err_msg=f"[{self.name}] Result mismatch",
        )


@dataclass
class FinderSpec:
    """Groups expectations for a single finder class."""

    finder_name: str
    mapping_file: str
    expectations: list[TestExpectation]
    # Keyed by backend name — skips every expectation in the spec (an expectation's own reason wins).
    skip_backends: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for expectation in self.expectations:
            expectation.skip_backends = {**self.skip_backends, **expectation.skip_backends}
