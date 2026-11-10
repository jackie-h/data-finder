"""Dev-time tool: are the ported specs still what Legend's tests assert?

Usage: uv run datafinder_legend_tests/tools/verify_port.py [--show-changed] [path-prefix]

Compares the SQL of every expectation in the specs (ported or recorded as unsupported) with the SQL its Legend test
asserts in the current checkout (extract_legend.py). Reports each as same, changed (the test now asserts other SQL),
or gone (no test of that file and name any more).
"""
from __future__ import annotations

import importlib
import pkgutil
import re
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
from datafinder_legend_tests.legend import BACKEND  # noqa: E402


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def specs() -> list:
    found = []
    for mod in pkgutil.walk_packages(datafinder_legend_tests.__path__, "datafinder_legend_tests."):
        if mod.name.endswith("_specs"):
            module = importlib.import_module(mod.name)
            for value in vars(module).values():
                if isinstance(value, FinderSpec):
                    for e in value.expectations:
                        found.append((mod.name, e))
    return found


def main(argv: list[str]) -> None:
    show = "--show-changed" in argv
    prefixes = [a for a in argv if not a.startswith("--")]
    catalogue: dict[str, dict[str, list[list[str]]]] = defaultdict(dict)
    for path in sorted(LEGEND_ROOT.rglob("*.pure")):
        rel = str(path.relative_to(LEGEND_ROOT))
        for t in extract_file(path, LEGEND_ROOT):
            catalogue[rel][t["name"]] = [[_flat(s) for s in a["sql"]] for a in t["expected_sql"]]
    counts = defaultdict(int)
    for module, e in specs():
        if not e.source or (prefixes and not any(e.source.startswith(p) for p in prefixes)):
            continue
        rel, _, name = e.source.partition("::")
        ours = _flat(e.equivalent_sql.get(BACKEND, ""))
        tests = catalogue.get(rel, {})
        if name not in tests:
            counts["gone"] += 1
            print(f"GONE     {e.source}")
        elif any(ours in sqls for sqls in tests[name]):
            counts["same"] += 1
        else:
            counts["changed"] += 1
            print(f"CHANGED  {e.source}")
            if show:
                print(f"    ours:   {ours}")
                for sqls in tests[name]:
                    for s in sqls:
                        print(f"    legend: {s}")
    print(dict(counts))


if __name__ == "__main__":
    main(sys.argv[1:])
