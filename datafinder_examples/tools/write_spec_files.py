"""Writes the spec files of datafinder_specs/specs: the JSON and CSV that each spec of a suite exports.

``uv run datafinder_examples/tools/write_spec_files.py`` writes them again from the Python specs. Run it after changing a spec of a suite
that is written as files (see ``suites()`` in spec_files.py).
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
