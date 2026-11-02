"""Writes datafinder_examples_tests/src/datafinder_examples_tests/tpch_bih_cases.py: the benchmark cases with their expected rows.

``uv run datafinder_examples/tools/build_tpch_bih_cases.py`` runs the report's SQL (tpch_bih_definitions.py) on the generated
data and writes what it returns. Run it after changing a definition, the generator or the seed.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "datafinder_examples_tests" / "src"))

from datafinder_examples_tests.tpch_bih_build import CASES_FILE, build  # noqa: E402

if __name__ == "__main__":
    CASES_FILE.write_text(build())
    print(f"wrote {CASES_FILE}")
