"""The tables of a suite: the statements of its schema.sql, and the CSV that holds the rows of each table (suite.json).

An engine runs the statements of ``schema.sql`` in its own spelling of the types and loads each table from its CSV: ``load_duckdb`` does it for
DuckDB, and ``tables`` gives the Spark harness the columns and the CSV of each table.
"""
from __future__ import annotations

import json
import re
from typing import Any

from datafinder_examples_tests.spec_files import specs_folder
from datafinder_examples_tests.tables import TableData

_TABLE = re.compile(r"^CREATE TABLE\s+(\S+)\s*\((.*)\)\s*$", re.DOTALL | re.IGNORECASE)


def statements(suite: str) -> list[str]:
    """The statements of the suite's schema.sql, without the comments."""
    text = "\n".join(line for line in (specs_folder() / suite / "schema.sql").read_text(encoding="utf-8").splitlines()
                     if not line.lstrip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def data(suite: str) -> dict[str, str]:
    """The CSV of each table of the suite, by the name of the table as the mapping reads it."""
    return json.loads((specs_folder() / suite / "suite.json").read_text(encoding="utf-8"))["data"]


def _csv(suite: str, name: str) -> str | None:
    return data(suite).get(name)


def tables(suite: str) -> list[TableData]:
    """The tables a suite creates, each with its columns and the CSV of its rows (none for a table with no rows). A table with no schema is in ``main``."""
    found = []
    for statement in statements(suite):
        match = _TABLE.match(statement)
        if match is not None:
            name, columns = match.groups()
            found.append(TableData(name if "." in name else f"main.{name}", columns, _csv(suite, name)))
    return found


def load_duckdb(connection: Any, suite: str) -> None:
    """Create the schemas and tables of the suite in a DuckDB connection, and load each table from its CSV."""
    from datafinder_examples import example_path
    rows = data(suite)
    for statement in statements(suite):
        connection.execute(re.sub(r"VECTOR\((\d+)\)", r"FLOAT[\1]", statement, flags=re.IGNORECASE))
        match = _TABLE.match(statement)
        if match is not None and match.group(1) in rows:
            name = match.group(1)
            connection.execute(f"INSERT INTO {name} SELECT * FROM read_csv_auto('{str(example_path(rows[name]))}')")
