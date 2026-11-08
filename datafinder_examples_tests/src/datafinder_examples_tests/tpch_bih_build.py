"""Builds tpch_bih_expected.py: the rows the SQL of each case in tpch_bih_definitions.py returns on the generated data.

``uv run datafinder_examples/tools/build_tpch_bih_cases.py`` writes the file; a test builds it again and compares, so the
checked-in expected rows can't drift from the SQL, the generator or the seed.
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import duckdb

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase
from datafinder_examples_tests.tpch_bih_data import csv_dir
from datafinder_examples_tests.tpch_bih_db import load
from datafinder_examples_tests.tpch_bih_definitions import DEFINITIONS as BIH_DEFINITIONS
from datafinder_examples_tests.tpch_definitions import TPCH_DEFINITIONS

EXPECTED_FILE = Path(__file__).with_name("tpch_bih_expected.py")

_HEADER = '''"""The rows each benchmark case returns on the generated data. Generated: do not edit.

Built by datafinder_examples/tools/build_tpch_bih_cases.py: each is what the SQL of the case with that id (tpch_bih_definitions.py or tpch_definitions.py) returns
on the data tpch_bih_generator.py makes (see tpch_bih_data.py).
"""
import datetime
from decimal import Decimal

ROWS = {
'''


def evaluate(definitions: list[BenchmarkCase] = [*BIH_DEFINITIONS, *TPCH_DEFINITIONS]) -> list[BenchmarkCase]:
    """Each definition with the rows its SQL returns."""
    connection = duckdb.connect()
    load(connection, csv_dir())
    cases = [dataclasses.replace(d, rows=tuple(d.run_sql(connection))) for d in definitions]
    connection.close()
    return cases


def _pack(items: list[str], indent: str, width: int = 118) -> str:
    lines, line = [], indent
    for item in items:
        if len(line) + len(item) + 2 > width and line.strip():
            lines.append(line.rstrip())
            line = indent
        line += item + ", "
    lines.append(line.rstrip())
    return "\n".join(lines)


def _render(case: BenchmarkCase) -> str:
    rows = _pack([repr(row) for row in case.rows], " " * 8)
    return f"    {case.id!r}: (\n{rows}\n    ),\n"


def render(cases: list[BenchmarkCase]) -> str:
    return _HEADER + "".join(_render(case) for case in cases) + "}\n"


def build() -> str:
    return render(evaluate())
