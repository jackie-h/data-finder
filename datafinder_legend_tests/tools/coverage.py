"""Dev-time tool: how much of legend-engine's projection SQL tests have been ported.

Usage: uv run datafinder_legend_tests/tools/coverage.py [--missing] [path-prefix]

Compares the catalogue of Legend tests (extract_legend.py) with the ``source`` of every expectation in the specs.
Unsupported expectations count as recorded, not ported. Only tests that project and assert SQL are counted.
"""
from __future__ import annotations

import importlib
import pkgutil
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
for src in ("datafinder_legend_tests/src", "datafinder_examples_tests/src", "datafinder/src", "model/src"):
    sys.path.insert(0, str(_REPO / src))
sys.path.insert(0, str(Path(__file__).parent))
from extract_legend import LEGEND_ROOT, extract_file  # noqa: E402

import datafinder_legend_tests  # noqa: E402
from datafinder_examples_tests.spec import FinderSpec  # noqa: E402


def ported_sources() -> dict[str, str]:
    """source -> 'ported' | 'unsupported'"""
    found: dict[str, str] = {}
    for mod in pkgutil.walk_packages(datafinder_legend_tests.__path__, "datafinder_legend_tests."):
        if not mod.name.endswith("_specs"):
            continue
        module = importlib.import_module(mod.name)
        for value in vars(module).values():
            if isinstance(value, FinderSpec):
                for e in value.expectations:
                    status = "unsupported" if e.skip_backends.get("duckdb", "").startswith("unsupported") else "ported"
                    found[e.source] = status
    return found


def main(argv: list[str]) -> None:
    show_missing = "--missing" in argv
    prefixes = [a for a in argv if not a.startswith("--")]
    done = ported_sources()
    per_file: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])  # total, ported, unsupported
    missing: dict[str, list[str]] = defaultdict(list)
    for path in sorted(LEGEND_ROOT.rglob("*.pure")):
        rel = str(path.relative_to(LEGEND_ROOT))
        if prefixes and not any(rel.startswith(p) for p in prefixes):
            continue
        for t in extract_file(path, LEGEND_ROOT):
            if t["kind"] != "project" or not t["expected_sql"]:
                continue
            row = per_file[rel]
            row[0] += 1
            status = done.get(f"{rel}::{t['name']}")
            if status == "ported":
                row[1] += 1
            elif status == "unsupported":
                row[2] += 1
            else:
                missing[rel].append(t["name"])
    total = [0, 0, 0]
    for rel, row in per_file.items():
        if row[1] or row[2] or prefixes:
            print(f"{rel:75} {row[1]:4} ported {row[2]:4} unsupported of {row[0]:4}")
        for i in range(3):
            total[i] += row[i]
        if show_missing:
            for name in missing[rel]:
                print(f"    - {name}")
    print(f"\nTOTAL: {total[1]} ported, {total[2]} recorded unsupported, of {total[0]} projection tests with SQL")


if __name__ == "__main__":
    main(sys.argv[1:])
