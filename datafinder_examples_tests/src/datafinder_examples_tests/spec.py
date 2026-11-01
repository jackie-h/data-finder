from __future__ import annotations

import math
from dataclasses import dataclass, field
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
class TestExpectation:
    """A single backend-agnostic query expectation."""

    name: str
    query: Callable  # (finder) -> FinderResult
    expected_columns: list[str]
    expected_result: np.ndarray
    # Keyed by backend name (e.g. "duckdb"). Omit to skip SQL check for that backend.
    expected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — fragment that must NOT appear (e.g. "JOIN", to assert join elision).
    unexpected_sql: dict[str, str] = field(default_factory=dict)
    # Keyed by backend name — reason the expectation does not apply to that backend. The run is skipped.
    skip_backends: dict[str, str] = field(default_factory=dict)
    # Compare floats to a relative 1e-9, not exactly: for an aggregate whose last digits depend on the order of the rows.
    approximate: bool = False

    def run(self, finder: Any, backend: str | None = None) -> None:
        if backend and backend in self.skip_backends:
            pytest.skip(self.skip_backends[backend])
        result = self.query(finder)
        if backend and (backend in self.expected_sql or backend in self.unexpected_sql):
            actual_sql = result.to_sql()
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
        df = result.to_pandas()
        assert list(df.columns) == self.expected_columns, (
            f"[{self.name}] Column mismatch.\n"
            f"Expected: {self.expected_columns}\n"
            f"Actual:   {list(df.columns)}"
        )
        if self.approximate:
            _assert_rows_close(_normalize_missing(df.values), _normalize_missing(self.expected_result), self.name)
            return
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
