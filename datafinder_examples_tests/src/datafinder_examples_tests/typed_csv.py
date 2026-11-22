"""
The expected result of a spec as a CSV that a person can read and any language can parse.

    id,name,category,distance
    Integer,String,String[0..1],Float[0..1]
    1,Acme Corp,Technology,0.25
    4,Delta Ltd,,

The first row is the name of each column. The second is its type, in the names of the model: Integer, Float, Decimal, String,
Boolean, StrictDate, DateTime and Vector, with ``[0..1]`` after the name of a column that has a missing value. A missing value is an
empty cell with no quotes, and the empty string is ``""``. A cell has no quotes unless it holds a comma, a quote or a line break, or
is the empty string. A date is ``2024-01-31``, a date and time ``2024-01-31T08:55:30``, and a vector ``[1.0,0.2,0.0]``.
"""
from __future__ import annotations

import datetime
import decimal
import json
from typing import Any

import numpy as np
import pandas as pd

_OPTIONAL = "[0..1]"


def column_type(values: list[Any]) -> str:
    """The type of a column, from the values the spec expects in it: the one type of the values that are present."""
    present = [_plain(v) for v in values if not _missing(v)]
    kinds = {_kind(v) for v in present}
    if not kinds:
        raise ValueError("A column with no value has no type to work out: declare it in expected_types")
    if kinds == {"Integer", "Float"}:
        kinds = {"Float"}
    if kinds == {"Integer", "Decimal"}:
        kinds = {"Decimal"}
    if len(kinds) != 1:
        raise ValueError(f"A column has values of more than one type: {sorted(kinds)}")
    name = kinds.pop()
    return name + _OPTIONAL if len(present) < len(values) else name


def _missing(value: Any) -> bool:
    """A value that is not there: None, NaN, pd.NA and the like."""
    if isinstance(value, (list, tuple, np.ndarray)):
        return False
    return bool(pd.isna(value))


def _plain(value: Any) -> Any:
    """A Python value for a numpy one."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (np.ndarray, tuple)):
        return [_plain(v) for v in value]
    return value


def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Float"
    if isinstance(value, decimal.Decimal):
        return "Decimal"
    if isinstance(value, str):
        return "String"
    if isinstance(value, datetime.datetime):
        return "DateTime"
    if isinstance(value, datetime.date):
        return "StrictDate"
    if isinstance(value, list):
        return "Vector"
    raise TypeError(f"A result cannot hold a {type(value).__name__}: {value!r}")


def _cell(value: Any, type_name: str) -> str:
    """The text of one cell, with the quotes it needs."""
    if _missing(value):
        return ""
    value = _plain(value)
    base = type_name.removesuffix(_OPTIONAL)
    if base == "Boolean":
        text = "true" if value else "false"
    elif base == "Float":
        text = repr(float(value))
    elif base in ("Integer", "Decimal", "String"):
        text = str(value)
    elif base == "StrictDate":
        text = value.isoformat()
    elif base == "DateTime":
        text = value.isoformat()
    elif base == "Vector":
        text = json.dumps([float(v) for v in value], separators=(",", ":"))
    else:
        raise ValueError(f"Unknown type {type_name}")
    if text == "" or any(c in text for c in ',"\r\n'):
        return '"' + text.replace('"', '""') + '"'
    return text


def _value(text: str, quoted: bool, type_name: str) -> Any:
    if text == "" and not quoted:
        return None
    base = type_name.removesuffix(_OPTIONAL)
    if base == "String":
        return text
    if base == "Integer":
        return int(text)
    if base == "Float":
        return float(text)
    if base == "Decimal":
        return decimal.Decimal(text)
    if base == "Boolean":
        if text not in ("true", "false"):
            raise ValueError(f"A boolean is true or false, not {text!r}")
        return text == "true"
    if base == "StrictDate":
        return datetime.date.fromisoformat(text)
    if base == "DateTime":
        return datetime.datetime.fromisoformat(text)
    if base == "Vector":
        return json.loads(text)
    raise ValueError(f"Unknown type {type_name}")


def _rows(text: str) -> list[list[tuple[str, bool]]]:
    """The rows of a CSV, each cell with whether it was quoted: an empty cell is a missing value only when it was not."""
    rows: list[list[tuple[str, bool]]] = []
    row: list[tuple[str, bool]] = []
    cell: list[str] = []
    quoted = False
    in_quotes = False
    i = 0
    while i < len(text):
        c = text[i]
        if in_quotes:
            if c == '"':
                if text[i + 1:i + 2] == '"':
                    cell.append('"')
                    i += 1
                else:
                    in_quotes = False
            else:
                cell.append(c)
        elif c == '"':
            in_quotes = quoted = True
        elif c == ",":
            row.append(("".join(cell), quoted))
            cell, quoted = [], False
        elif c in "\r\n":
            if c == "\r" and text[i + 1:i + 2] == "\n":
                i += 1
            row.append(("".join(cell), quoted))
            rows.append(row)
            row, cell, quoted = [], [], False
        else:
            cell.append(c)
        i += 1
    if cell or quoted or row:
        row.append(("".join(cell), quoted))
        rows.append(row)
    return rows


def write(columns: list[str], types: list[str], rows: Any) -> str:
    """The CSV of a result: the names, the types, then a row for each row of the result."""
    lines = [",".join(_cell(c, "String") for c in columns), ",".join(types)]
    for row in rows:
        lines.append(",".join(_cell(v, t) for v, t in zip(row, types)))
    return "\n".join(lines) + "\n"


def read(text: str) -> tuple[list[str], list[str], np.ndarray]:
    """The column names, the types, and the rows as an array of the values the types name."""
    rows = _rows(text)
    if len(rows) < 2:
        raise ValueError("A result has a row of names and a row of types")
    columns = [c for c, _ in rows[0]]
    types = [c for c, _ in rows[1]]
    values = np.empty((len(rows) - 2, len(columns)), dtype=object)
    for r, row in enumerate(rows[2:]):
        if len(row) != len(columns):
            raise ValueError(f"Row {r + 1} has {len(row)} cells, and there are {len(columns)} columns")
        for c, (cell, quoted) in enumerate(row):
            values[r, c] = _value(cell, quoted, types[c])
    return columns, types, values

