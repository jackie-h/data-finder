"""Seed a DuckDB database with test data taken from legend-engine, for specs that run their queries."""
from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb

from datafinder import QueryRunnerBase
from datafinder_ibis.ibis_engine import IbisConnect


def statements(sql_file: Path) -> list[str]:
    """The statements in a data file, one per line; lines starting with -- are comments."""
    lines = [line.strip() for line in sql_file.read_text().splitlines()]
    return [line.rstrip(";") for line in lines if line and not line.startswith("--")]


@contextmanager
def seeded_database(*data_files: Path):
    """Run the specs' queries against a fresh ``test.db`` in a temporary directory, seeded from ``data_files``.

    The engines connect to ``test.db`` relative to the working directory, so the directory is changed for the
    duration and restored after, leaving any other ``test.db`` alone.
    """
    previous = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        os.chdir(tmp)
        try:
            conn = duckdb.connect("test.db")
            try:
                for data_file in data_files:
                    for statement in statements(data_file):
                        conn.execute(statement)
            finally:
                conn.close()
            QueryRunnerBase.clear()
            QueryRunnerBase.register(IbisConnect)
            yield Path(tmp) / "test.db"
        finally:
            os.chdir(previous)
