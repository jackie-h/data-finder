"""Generate finders from a mapping markdown file, for SQL-only specs (no database is created or seeded)."""
from __future__ import annotations

import shutil
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from datafinder_generator.generator import generate
from mapping_markdown.markdown_mapping import load


@contextmanager
def generated_finders(mapping_file: Path):
    """Generate the finders for ``mapping_file`` and put them on ``sys.path``; imports are undone on exit."""
    tmp = tempfile.mkdtemp()
    before = set(sys.modules)
    sys.path.insert(0, tmp)
    try:
        generate(load(str(mapping_file)), tmp)
        yield
    finally:
        if tmp in sys.path:
            sys.path.remove(tmp)
        for name in set(sys.modules) - before:
            del sys.modules[name]
        shutil.rmtree(tmp, ignore_errors=True)
