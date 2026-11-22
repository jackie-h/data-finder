"""A table of the example data and the CSV that holds its rows: what an engine loads before it runs the specs. The tables of each suite are in its schema.sql (see suite_data).

The column types are the ones every database spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and ``VECTOR(n)``, a vector of n numbers written
in the CSV as ``[0.5,0.25]``, which an engine loads as its own array of floats; the rows are in the CSV in the order of the columns.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TableData:
    name: str  # as the mapping reads it, with its schema: ``trading.trades``, or ``main.companies`` where the mapping gives none (``main`` is the default schema)
    columns: str  # the definition of the columns: ``id INT, name VARCHAR``
    csv: str | None = None  # the file in datafinder_specs/specs/data with its rows (with a header); none for a table with no rows
