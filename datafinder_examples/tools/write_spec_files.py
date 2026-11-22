"""Writes the JSON of each spec of datafinder_specs/specs: its query, finder and mapping, from the Python spec.

``uv run datafinder_examples/tools/write_spec_files.py`` writes them again. Run it after adding or changing the query of a spec of a suite
(see ``SUITES`` in spec_files.py); what else a spec expects is in its CSV and SQL files, which are not written from the Python.
"""
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent.parent
for each in ("datafinder/src", "model/src", "datafinder_examples/src", "datafinder_examples_tests/src"):
    sys.path.insert(0, str(root / each))

from datafinder_examples_tests.spec_files import specs_folder, write_all  # noqa: E402

if __name__ == "__main__":
    write_all()
    print(f"wrote {specs_folder()}")
