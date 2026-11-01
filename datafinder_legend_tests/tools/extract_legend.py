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

_FUNC_RE = re.compile(r"^[ \t]*function\s+<<([^>]*test\.[^>]*)>>\s+([\w:]+)\s*\(", re.M)
_ANY_FUNC_RE = re.compile(r"^[ \t]*function\s", re.M)


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
        # assertSameSQL(expectedSql, $result) takes the result itself rather than its sql
        if m.group(1) != "assertSameSQL" and not any("sqlRemoveFormatting" in a for a in args):
            continue
        if m.group(1) == "assertSameSQL" and not any(a.strip().startswith("$") for a in args[1:2]):
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


def _top_level_calls(query: str) -> list[str]:
    """The names of the functions chained at the top level of the query, ``a->f()->g(x->h())`` giving f and g."""
    text = query.strip()
    if text.startswith("{|"):  # a lambda around the query; what follows its closing brace is chained on it too
        depth, end = 0, len(text)
        for i, ch in enumerate(text):
            depth += ch == "{"
            depth -= ch == "}"
            if depth == 0:
                end = i
                break
        text = text[2:end] + text[end + 1:]
    calls, depth, i = [], 0, 0
    while i < len(text):
        ch = text[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and text.startswith("->", i):
            m = re.match(r"->\s*(?:[\w:]*::)?(\w+)\s*\(", text[i:])
            if m:
                calls.append(m.group(1))
        i += 1
    return calls


def _classify(query: str) -> str:
    """``project`` for a query whose result is a table: it projects or groups at the top level."""
    if not query.strip() or {"project", "groupBy"} & set(_top_level_calls(query)):
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
            "qualified": header.group(2),
            "tags": header.group(1),
            "executions": execs,
            "kind": _classify(execs[0]["query"]) if execs else "other",
            "expected_sql": sqls,
        })
    _qualify_repeated_names(tests)
    return tests


def _qualify_repeated_names(tests: list[dict]) -> None:
    """A file can hold several projection tests with the same last name in different Pure packages
    (``rtrim::testProject`` and ``left::testProject``). Each gets as many trailing package segments as make its name
    unique, so a test is identified by file and name. Only those tests, the ones the port is keyed on, are renamed: a
    name that is not repeated among them stays the bare function name."""
    by_name: dict[str, list[dict]] = {}
    for test in tests:
        if test["kind"] == "project" and test["expected_sql"]:
            by_name.setdefault(test["name"], []).append(test)
    for group in by_name.values():
        if len(group) == 1:
            continue
        parts = [t["qualified"].split("::") for t in group]
        for depth in range(2, max(len(p) for p in parts) + 1):
            names = ["::".join(p[-depth:]) for p in parts]
            if len(set(names)) == len(names):
                for test, name in zip(group, names):
                    test["name"] = name
                break


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
