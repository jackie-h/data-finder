"""
SQL specs ported from the relational tests in legend-engine (core_relational).

The folder layout mirrors the Legend source. Each spec module is named after the Legend test file it ports, and
each expectation records the Legend function (``source``) and the SQL Legend asserts. Only the tabular
``project(...)`` tests are ported; object and graph-fetch queries are out of scope.
"""
from pathlib import Path

LEGEND_TESTS_DIR = Path(__file__).parent


def legend_path(relative: str) -> Path:
    """Path to a model or mapping markdown file under this package, e.g. ``tests/simple_mapping.md``."""
    return LEGEND_TESTS_DIR / relative
