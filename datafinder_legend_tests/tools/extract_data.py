"""Dev-time tool: pull the test data for some tables out of a legend-engine setup function.

Usage: uv run datafinder_legend_tests/tools/extract_data.py <pure file> <function> <table>... > data.sql
(tables in another schema are named schema.table)

The Legend setup functions run SQL through ``executeInDb('...', $connection)`` and call other ``...AndFillDb``
functions. This follows those calls and prints, in order, the statements (drop, create, insert) that touch the named
tables; name a table in another schema as schema.table, ready to run against DuckDB.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from extract_legend import LEGEND_ROOT, _read_string, _strip_comments

_STEP = re.compile(r"executeInDb\s*\(\s*'|(?:[\w:]*::)?(\w*AndFillDb|initDatabase)\s*\(\s*\$connection"
                   r"|dropAndCreateTableInDb\s*\(\s*\w+\s*,\s*'(\w+)'")
_DB_TABLE = re.compile(r"^\s*Table\s+(\w+)\s*\((.*)\)\s*$", re.M)
_TABLE = re.compile(r"^\s*(?:drop\s+table\s+if\s+exists|create\s+table|insert\s+into)\s+((?:\w+\.)?\w+)", re.I)
_SCHEMA = re.compile(r"^\s*(?:drop\s+schema\s+if\s+exists|create\s+schema)\s+(\w+)", re.I)


def _function_bodies(root: Path) -> dict[str, str]:
    bodies: dict[str, str] = {}
    for path in root.rglob("*.pure"):
        text = _strip_comments(path.read_text())
        starts = [m for m in re.finditer(r"^function\s", text, re.M)]
        for i, m in enumerate(starts):
            end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
            name = re.match(r"function\s+(?:<<[^>]*>>\s+)?([\w:]+)\s*\(", text[m.start():])
            if name:
                bodies.setdefault(name.group(1).split("::")[-1], text[m.start():end])
    return bodies


def _database_tables(root: Path) -> dict[str, str]:
    """Table name -> column list, from the single-line ``Table name(...)`` definitions of the Legend database."""
    text = (root / "tests" / "relationalSetUp.pure").read_text()
    return {m.group(1).lower(): m.group(2) for m in _DB_TABLE.finditer(text)}


def statements(function: str, bodies: dict[str, str], tables: dict[str, str],
               seen: set[str] | None = None) -> list[str]:
    seen = seen if seen is not None else set()
    if function in seen or function not in bodies:
        return []
    seen.add(function)
    body = bodies[function]
    out: list[str] = []
    for m in _STEP.finditer(body):
        if m.group(1):
            out.extend(statements(m.group(1), bodies, tables, seen))
        elif m.group(2):  # the table is created from its definition in the Legend database
            name = m.group(2)
            out.append(f"Drop table if exists {name}")
            out.append(f"Create Table {name}({tables[name.lower()]})")
        else:
            sql, _ = _read_string(body, m.end() - 1)
            out.append(sql.strip().rstrip(";"))
    return out


def main(argv: list[str]) -> None:
    pure_file, function, *tables = argv
    wanted = {t.lower() for t in tables}
    bodies = _function_bodies(LEGEND_ROOT)
    print(f"-- Test data from legend-engine core_relational {Path(pure_file).name}::{function}")
    print(f"-- Tables: {', '.join(tables)}")
    for sql in statements(function, bodies, _database_tables(LEGEND_ROOT)):
        table, schema = _TABLE.match(sql), _SCHEMA.match(sql)
        if table and table.group(1).lower() in wanted:
            print(sql + ";")
        elif schema and any(name.startswith(schema.group(1).lower() + ".") for name in wanted):
            print(sql.replace(" cascade", "") + ";")


if __name__ == "__main__":
    main(sys.argv[1:])
