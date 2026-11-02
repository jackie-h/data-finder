"""Builds tpch_bih_cases.py: the benchmark cases of tpch_bih_definitions.py with the rows their SQL returns on the generated data.

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
from datafinder_examples_tests.tpch_bih_definitions import DEFINITIONS

CASES_FILE = Path(__file__).with_name("tpch_bih_cases.py")

_HEADER = '''"""The TPC-BiH benchmark cases with their expected rows. Generated: do not edit.

Built by datafinder_examples/tools/build_tpch_bih_cases.py from tpch_bih_definitions.py (the report's SQL and the values of its
parameters): each case's rows are what its SQL returns on the data tpch_bih_generator.py makes (see tpch_bih_data.py).
"""
import datetime
from decimal import Decimal

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase

CASES = {case.id: case for case in [
'''


def evaluate(definitions: list[BenchmarkCase] = DEFINITIONS) -> list[BenchmarkCase]:
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
    parameters = ", ".join(f"{name!r}: {value!r}" for name, value in case.parameters.items())
    flags = "".join(f", {flag}=True" for flag in ("ordered", "distinct", "approximate") if getattr(case, flag))
    rows = _pack([repr(row) for row in case.rows], " " * 12)
    return (f"    BenchmarkCase(\n        id={case.id!r}, title={case.title!r},\n"
            f"        sql=\"\"\"{case.sql}\"\"\",\n        parameters={{{parameters}}},\n"
            f"        select={case.select!r}{flags},\n        rows=(\n{rows}\n        )),\n")


def render(cases: list[BenchmarkCase]) -> str:
    return _HEADER + "\n".join(_render(case) for case in cases) + "]}\n"


def build() -> str:
    return render(evaluate())
