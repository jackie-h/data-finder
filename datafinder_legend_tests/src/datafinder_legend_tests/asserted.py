"""Compare what a spec's query returns with the rows that Legend's own test asserts (``asserted_rows.json``, from tools/extract_rows.py).

The rows Legend asserts are the oracle: they come from Legend running its test on its data, not from our reading of its SQL.
Each claim of a test is checked: the whole result, some rows of it, every cell of it, one column of it, or the number of rows.
"""
from __future__ import annotations

import datetime
import json
import math
import re
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import Any

_JSON = Path(__file__).with_name("asserted_rows.json")


@cache
def _all() -> dict:
    return json.loads(_JSON.read_text())


def claims_of(source: str) -> list[dict]:
    """The claims about the rows of the test ``source`` (``file::name``), or none."""
    return _all().get(source, {}).get("claims", [])


def _as_datetime(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)
    if isinstance(value, str):
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?(?:Z|[+-]\d{2}:?\d{2})?", value.strip())
        if m:
            y, mo, d, h, mi, s, frac = m.groups()
            return datetime.datetime(int(y), int(mo), int(d), int(h or 0), int(mi or 0), int(s or 0), int((frac or "0")[:6].ljust(6, "0")))
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", value.strip()):
        return float(value)
    return None


def same(actual: Any, expected: Any) -> bool:
    """One value of ours is one value of Legend's: a missing value, a boolean, a number (to 1e-9), a date or text."""
    if isinstance(expected, dict):
        return _as_datetime(actual) == _as_datetime(expected["d"])
    if expected is None or (isinstance(expected, str) and expected in ("TDSNull", "null")):
        return actual is None
    if actual is None:
        return expected == ""  # a CSV writes a missing value as nothing
    if isinstance(expected, bool) or expected in ("true", "false"):
        return isinstance(actual, bool) and actual == (expected is True or expected == "true")
    if isinstance(actual, bool):
        return False
    a, e = _number(actual), _number(expected)
    if a is not None and e is not None:
        return math.isclose(a, e, rel_tol=1e-9, abs_tol=1e-9)
    ad, ed = _as_datetime(actual), _as_datetime(expected)
    if ad is not None and ed is not None and not (isinstance(actual, str) and isinstance(expected, str)):
        return ad == ed
    return str(actual) == str(expected)


def _row_same(actual: tuple, expected: list) -> bool:
    return len(actual) == len(expected) and all(same(a, e) for a, e in zip(actual, expected))


def _match(actual: list, expected: list, same_fn) -> tuple[list, list]:
    """What is left of each after pairing every expected item with a different one of ours that is the same."""
    left = list(actual)
    missing = []
    for e in expected:
        for i, a in enumerate(left):
            if same_fn(a, e):
                del left[i]
                break
        else:
            missing.append(e)
    return left, missing


def check(source: str, rows: list[tuple], width: int) -> None:
    """Raise AssertionError if our ``rows`` are not what the claims of ``source`` say. No claims is no check."""
    for claim in claims_of(source):
        kind = claim["kind"]
        context = f"[{source}] rows differ from what Legend's test asserts ({kind}).\n  Legend: "
        if kind == "count":
            assert len(rows) == claim["n"], f"{context}{claim['n']} rows\n  Ours ({len(rows)}): {rows}"
        elif kind in ("rows", "subset"):
            expected = claim["rows"]
            if "sep" in claim:  # Legend's makeString: a value can hold the separator, so the row is split into as many cells as we have
                expected = [r.split(claim["sep"], width - 1) if width else r.split(claim["sep"]) for r in expected]
            left, missing = _match(rows, expected, _row_same)
            if claim.get("csv") and (missing or left):  # Legend's toCSV can end with a blank line, which is not a row
                while expected and expected[-1] == [""]:
                    expected = expected[:-1]
                    left, missing = _match(rows, expected, _row_same)
                    if not missing and not left:
                        break
            assert not missing and (kind == "subset" or not left), (
                f"{context}{expected}\n  Ours ({len(rows)}): {rows}\n  Not in ours: {missing}\n  Not in Legend's: {left if kind == 'rows' else '(not asserted)'}")
        elif kind == "flat":
            values = claim["values"]
            if claim.get("sorted") or width == 0:
                cells = [c for row in rows for c in row]
                left, missing = _match(cells, values, same)
                assert not missing and not left, f"{context}{values}\n  Ours: {cells}\n  Not in ours: {missing}\n  Not in Legend's: {left}"
            else:
                assert width and len(values) % width == 0, f"{context}{len(values)} values in rows of {width}\n  Ours ({len(rows)}): {rows}"
                expected = [values[i:i + width] for i in range(0, len(values), width)]
                left, missing = _match(rows, expected, _row_same)
                assert not missing and not left, f"{context}{expected}\n  Ours ({len(rows)}): {rows}\n  Not in ours: {missing}\n  Not in Legend's: {left}"
        elif kind == "column":
            values = claim["values"]
            columns = [[row[i] for row in rows] for i in range(width)]
            results = [_match(c, values, same) for c in columns]
            assert any(not missing and not left for left, missing in results), f"{context}{values}\n  Ours: {rows}"
