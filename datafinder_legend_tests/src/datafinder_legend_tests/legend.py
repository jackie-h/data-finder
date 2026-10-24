"""Helpers for porting Legend tests into specs."""
from __future__ import annotations

from datafinder_examples_tests.spec import TestExpectation

BACKEND = "duckdb"

# Differences from Legend that the strict comparison reports. The SQL means the same; the text does not match.
JOIN_OPERAND_ORDER = (
    "to-one join conditions are written child.fk = parent.pk; Legend writes parent.pk = child.fk"
)


def _unsupported_query(finder):
    raise NotImplementedError


def ported(name: str, source: str, legend_sql: str, query, **kwargs) -> TestExpectation:
    """A SQL-only expectation: ``query`` must produce SQL equivalent to what Legend asserts."""
    return TestExpectation(name=name, query=query, source=source,
                           equivalent_sql={BACKEND: legend_sql}, **kwargs)


def unsupported(name: str, source: str, legend_sql: str, reason: str) -> TestExpectation:
    """A Legend test that cannot be expressed with the finder API yet. Kept so the gap stays visible."""
    return TestExpectation(name=name, query=_unsupported_query, source=source,
                           equivalent_sql={BACKEND: legend_sql}, skip_backends={BACKEND: f"unsupported: {reason}"})
