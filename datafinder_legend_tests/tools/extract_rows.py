"""Dev-time tool: the rows that each ported Legend test asserts, written to ``asserted_rows.json``.

Usage: uv run datafinder_legend_tests/tools/extract_rows.py

Legend's tests assert their result in many shapes. Each assertion that can be read is turned into a claim about the rows:

    rows      the whole result: a list of rows (``$tds.rows->at(0).values`` for each row, a ``toCSV`` string, rows joined with makeString)
    subset    rows that are in the result, when the test asserts only some of them
    flat      every cell of the result, row after row (``$result.values.rows.values``)
    column    the values of one column, in any order (``rows.getString('name')``, ``$result.values.name``)
    count     the number of rows

A claim whose shape is not read is left out and counted, so that coverage can be seen. A value is a string, number, boolean or
null, or ``{"d": "2015-08-15T00:00:00"}`` for a date; the cells of a ``makeString`` or CSV are text.
"""
from __future__ import annotations

import csv
import io
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from extract_legend import LEGEND_ROOT, _ANY_FUNC_RE, _FUNC_RE, _split_args, _strip_comments, extract_file  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "src" / "datafinder_legend_tests" / "asserted_rows.json"


class Unsupported(Exception):
    pass


_TOKEN = re.compile(r"""\s*(?:
    (?P<str>'(?:[^'\\]|\\.)*')
  | (?P<date>%[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}(?:T[0-9:.]+(?:[+-][0-9]{4})?)?)
  | (?P<num>-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)
  | (?P<null>\^(?:meta::pure::tds::)?TDSNull\(\))
  | (?P<bool>true|false)
  | (?P<punct>[\[\],+])
)""", re.X)

_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "'": "'", "\\": "\\", '"': '"'}


def _unquote(token: str) -> str:
    out, i, body = [], 0, token[1:-1]
    while i < len(body):
        if body[i] == "\\" and i + 1 < len(body):
            out.append(_ESCAPES.get(body[i + 1], body[i + 1]))
            i += 2
        else:
            out.append(body[i])
            i += 1
    return "".join(out)


def _date(token: str) -> dict:
    m = re.match(r"%(\d{4})-(\d{1,2})-(\d{1,2})(?:T(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?)?", token)
    if not m:
        raise Unsupported(token)
    y, mo, d, h, mi, s, frac = m.groups()
    text = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    if h is not None:
        text += f"T{int(h):02d}:{mi}:{s}"
        if frac and int(frac):
            text += "." + frac[:6].ljust(6, "0")
    return {"d": text}


def pure_value(source: str):
    """A Pure literal: a string (joined with ``+``), number, boolean, date, ``^TDSNull()`` or a list of them."""
    tokens, pos = [], 0
    source = source.strip()
    while pos < len(source):
        m = _TOKEN.match(source, pos)
        if not m or m.end() == pos:
            raise Unsupported(source[pos:pos + 30])
        assert m.lastgroup is not None, "every token pattern is a named group"
        tokens.append((m.lastgroup, m.group(m.lastgroup)))
        pos = m.end()
    i = 0

    def item():
        nonlocal i
        if i >= len(tokens):
            raise Unsupported("end")
        kind, text = tokens[i]
        i += 1
        if kind == "punct" and text == "[":
            values = []
            while True:
                if i < len(tokens) and tokens[i] == ("punct", "]"):
                    i += 1
                    return values
                values.append(term())
                if i < len(tokens) and tokens[i] == ("punct", ","):
                    i += 1
        i -= 1
        return term()

    def atom():
        nonlocal i
        kind, text = tokens[i]
        i += 1
        if kind == "str":
            return _unquote(text)
        if kind == "num":
            return float(text) if re.search(r"[.eE]", text) else int(text)
        if kind == "bool":
            return text == "true"
        if kind == "date":
            return _date(text)
        if kind == "null":
            return None
        if kind == "punct" and text == "[":
            i -= 1
            return item()
        raise Unsupported(text)

    def term():
        nonlocal i
        value = atom()
        while i < len(tokens) and tokens[i] == ("punct", "+") and isinstance(value, str):
            i += 1
            nxt = atom()
            if not isinstance(nxt, str):
                raise Unsupported("+")
            value += nxt
        return value

    result = item()
    if i != len(tokens):
        raise Unsupported("trailing")
    return result


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _as_list(value) -> list:
    values = value if isinstance(value, list) else [value]
    if any(isinstance(v, list) for v in values):
        raise Unsupported("nested")
    return values


def _claim(expected_src: str, actual_src: str, bag: bool = False):
    """One claim for ``assertEquals(expected, actual)``, ``("row", index, values)`` for a single row, or None when the shape is not read."""
    actual = _norm(actual_src)
    try:
        claim = _claim_of(pure_value(expected_src), actual)
        if bag and isinstance(claim, dict) and claim["kind"] == "flat":
            claim["sorted"] = True  # assertSameElements: the cells in any order, so no rows to chunk them into
        return claim
    except Unsupported:
        return None


def _claim_of(expected, actual: str):
    # the number of rows
    if re.search(r"\.rows\s*->\s*size\(\)$", actual) or re.fullmatch(r"\$\w+(?:\.values(?:->at\(\d+\)|->first\(\))?)?\.rows", actual):
        if isinstance(expected, int) and not isinstance(expected, bool):
            return {"kind": "count", "n": expected}
        return None
    # one row
    m = re.search(r"\.rows\s*->\s*at\((\d+)\)\.values$", actual)
    if m and isinstance(expected, list) and not any(isinstance(v, list) for v in expected):
        return ("row", int(m.group(1)), expected)
    # rows as joined text
    m = re.search(r"\.rows\s*->\s*map\(\s*r\s*\|\s*\$r\.values\s*->\s*makeString\('((?:[^'\\]|\\.)*)'\)\s*\)\s*(->\s*sort\(\)\s*)?(?:->\s*makeString\('((?:[^'\\]|\\.)*)'\)\s*)?$", actual)
    if m:
        sep, rowsep = _unquote("'" + m.group(1) + "'"), m.group(3)
        rowsep = None if rowsep is None else _unquote("'" + rowsep + "'")
        if rowsep is not None:
            text = expected[0] if isinstance(expected, list) and len(expected) == 1 else expected
            if not isinstance(text, str):
                return None
            rows = text.split(rowsep) if text != "" else []
        else:
            if not isinstance(expected, list) or not all(isinstance(r, str) for r in expected):
                return None
            rows = expected
        return {"kind": "rows", "rows": rows, "sep": sep}
    # a CSV: header, then the rows
    if re.search(r"toCSV\(", actual) and "replace(" not in actual:
        text = expected[0] if isinstance(expected, list) and len(expected) == 1 else expected
        if not isinstance(text, str):
            return None
        lines = list(csv.reader(io.StringIO(text)))
        if not lines:
            return None
        return {"kind": "rows", "rows": [r or [""] for r in lines[1:]], "header": lines[0], "csv": True}
    # every value of every row, row after row
    m = re.search(r"\.rows\s*(?:\.values|->\s*map\(\s*r\s*\|\s*\$r\.values\s*\))\s*(->\s*sort\(\)\s*)?(?:->\s*makeString\('((?:[^'\\]|\\.)*)'\)\s*)?$", actual)
    if m:
        if m.group(2) is not None:
            sep = _unquote("'" + m.group(2) + "'")
            text = expected[0] if isinstance(expected, list) and len(expected) == 1 else expected
            if not isinstance(text, str):
                return None
            return {"kind": "flat", "values": text.split(sep) if text != "" else [], "text": True, "sorted": bool(m.group(1))}
        return {"kind": "flat", "values": _as_list(expected), "sorted": bool(m.group(1))}
    # one column
    if (re.search(r"\.rows\s*(?:->\s*map\(\s*r\s*\|\s*\$r\.(?:get\w*\('[^']*'\)|values\s*->\s*at\(\d+\))\s*\)|\.get\w*\('[^']*'\))$", actual)
            or re.fullmatch(r"\$result\.values\.\w+", actual)):
        return {"kind": "column", "values": _as_list(expected)}
    return None


def _first_result(body: str) -> str:
    """The body from its first query to the second: the spec is the first query, and what is asserted after another is about that one."""
    starts = [m.start() for m in re.finditer(r"\b(?:execute|executeInDb|validate)\s*\(", body)]
    if not starts:
        return ""
    return body[starts[0]:starts[1]] if len(starts) > 1 else body[starts[0]:]


def asserted(body: str) -> dict | None:
    """What the test body asserts about the rows of its first query, or None."""
    body = _first_result(body)
    claims, rows_at = [], {}
    for m in re.finditer(r"\b(assertEquals|assertSameElements|assertSize)\s*\(", body):
        try:
            args, _ = _split_args(body, m.end() - 1)
        except ValueError:
            continue
        if len(args) < 2 or any("sqlRemoveFormatting" in a for a in args):
            continue
        first, second = args[0], args[1]
        if m.group(1) == "assertSize":
            expected_src, actual_src = second, first
            if not re.search(r"\.rows$", _norm(actual_src)):
                continue
            actual_src += "->size()"
        elif "$" in first and "$" not in second:
            expected_src, actual_src = second, first
        else:
            expected_src, actual_src = first, second
        c = _claim(expected_src, actual_src, bag=m.group(1) == "assertSameElements")
        if c is None:
            continue
        if isinstance(c, tuple):
            rows_at[c[1]] = c[2]
        else:
            claims.append(c)
    if rows_at:
        n = len(rows_at)
        ordered = [rows_at[i] for i in sorted(rows_at)]
        if sorted(rows_at) == list(range(n)) and any(c["kind"] == "count" and c["n"] == n for c in claims):
            claims.append({"kind": "rows", "rows": ordered})
        else:
            claims.append({"kind": "subset", "rows": ordered})
    return {"claims": claims} if claims else None


def main() -> None:
    result, kinds, tests = {}, Counter(), 0
    for path in sorted(LEGEND_ROOT.rglob("*.pure")):
        text = _strip_comments(path.read_text())
        funcs = list(_ANY_FUNC_RE.finditer(text))
        rel = str(path.relative_to(LEGEND_ROOT))
        names = iter(t["name"] for t in extract_file(path, LEGEND_ROOT))  # the names the specs use, repeated ones qualified
        for idx, fm in enumerate(funcs):
            header = _FUNC_RE.match(text, fm.start())
            if not header or "test.Test" not in header.group(1):
                continue
            end = funcs[idx + 1].start() if idx + 1 < len(funcs) else len(text)
            found = asserted(text[fm.start():end])
            tests += 1
            name = next(names)
            if found:
                result[f"{rel}::{name}"] = found
                for c in found["claims"]:
                    kinds[c["kind"]] += 1
    OUT.write_text(json.dumps(result, indent=0, sort_keys=True) + "\n")
    print(f"{len(result)} of {tests} tests have a claim; claims by kind: {dict(kinds)}")


if __name__ == "__main__":
    main()
