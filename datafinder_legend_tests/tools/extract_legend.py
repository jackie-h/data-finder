"""Dev-time tool: catalogue the SQL-asserting projection tests in legend-engine's core_relational tests.

Usage: uv run datafinder_legend_tests/tools/extract_legend.py [legend_dir ...] > catalogue.json

For every ``<<test.Test>>`` function it records the query, the mapping and each expected SQL string, and
classifies the test (``project`` / ``object`` / ``other``) so we know what is in scope. It does not run
Legend; the expected SQL is taken verbatim from the assertions.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

LEGEND_ROOT = Path.home() / "projects/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational"

_FUNC_RE = re.compile(r"^function\s+<<([^>]*test\.[^>]*)>>\s+([\w:]+)\s*\(", re.M)
_ANY_FUNC_RE = re.compile(r"^function\s", re.M)


def _read_string(text: str, i: int) -> tuple[str, int]:
    """Read a Pure single-quoted string starting at text[i] == "'"; returns (value, index after)."""
    assert text[i] == "'"
    out = []
    i += 1
    while i < len(text) and text[i] != "'":
        if text[i] == "\\" and i + 1 < len(text) and text[i + 1] in "'\\":
            out.append(text[i + 1])
            i += 2
        else:
            out.append(text[i])
            i += 1
    return "".join(out), i + 1


def _split_args(text: str, start: int) -> tuple[list[str], int]:
    """Split the argument list of a call whose '(' is at text[start]; returns (args, index after ')')."""
    depth = 0
    args, cur = [], []
    i = start
    while i < len(text):
        c = text[i]
        if c == "'":
            s, j = _read_string(text, i)
            cur.append(text[i:j])
            i = j
            continue
        if c in "([{":
            depth += 1
            if depth > 1:
                cur.append(c)
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                args.append("".join(cur).strip())
                return args, i + 1
            cur.append(c)
        elif c == "," and depth == 1:
            args.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    raise ValueError("unbalanced call")


def _strip_comments(text: str) -> str:
    out, i = [], 0
    while i < len(text):
        if text[i] == "'":
            _, j = _read_string(text, i)
            out.append(text[i:j])
            i = j
        elif text.startswith("//", i):
            while i < len(text) and text[i] != "\n":
                i += 1
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = len(text) if j < 0 else j + 2
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def _expected_sql(body: str) -> list[dict]:
    """Every string asserted against a sqlRemoveFormatting()/sql result in the body."""
    found = []
    for m in re.finditer(r"\b(assertEquals|assertEqualsH2Compatible|assertSameSQL)\s*\(", body):
        args, _ = _split_args(body, m.end() - 1)
        if not any("sqlRemoveFormatting" in a or "sqlRemoveFormatting" in a for a in args):
            continue
        strings = [a for a in args if a.startswith("'")]
        decoded = [_read_string(a, 0)[0] for a in strings if a.endswith("'")]
        if decoded:
            found.append({"assert": m.group(1), "sql": decoded})
    return found


def _executions(body: str) -> list[dict]:
    calls = []
    for m in re.finditer(r"\b(execute|executeInDb|validate)\s*\(", body):
        args, _ = _split_args(body, m.end() - 1)
        if len(args) >= 2:
            calls.append({"fn": m.group(1), "query": args[0], "mapping": args[1]})
    return calls


def _classify(query: str) -> str:
    if re.search(r"->\s*(project|groupBy|distinct)\s*\(", query) or "TabularDataSet" in query or "tds" in query.lower():
        return "project"
    if "graphFetch" in query or "serialize" in query:
        return "object"
    return "other"


def extract_file(path: Path, root: Path) -> list[dict]:
    text = _strip_comments(path.read_text())
    funcs = list(_ANY_FUNC_RE.finditer(text))
    tests = []
    for idx, fm in enumerate(funcs):
        end = funcs[idx + 1].start() if idx + 1 < len(funcs) else len(text)
        header = _FUNC_RE.match(text, fm.start())
        if not header or "test.Test" not in header.group(1):
            continue
        body = text[fm.start():end]
        execs = _executions(body)
        sqls = _expected_sql(body)
        tests.append({
            "file": str(path.relative_to(root)),
            "name": header.group(2).split("::")[-1],
            "tags": header.group(1),
            "executions": execs,
            "kind": _classify(execs[0]["query"]) if execs else "other",
            "expected_sql": sqls,
        })
    return tests


def main(argv: list[str]) -> None:
    dirs = [Path(a) for a in argv] or [LEGEND_ROOT]
    root = LEGEND_ROOT
    tests = []
    for d in dirs:
        files = [d] if d.is_file() else sorted(d.rglob("*.pure"))
        for f in files:
            tests.extend(extract_file(f, root))
    json.dump(tests, sys.stdout, indent=1)


if __name__ == "__main__":
    main(sys.argv[1:])
