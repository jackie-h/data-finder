"""The data the TPC-BiH cases run on: generated (see tpch_bih_generator.py) the first time it is asked for, and written to the CSV
files the runner loads."""
import atexit
import datetime
import functools
import shutil
import tempfile
from pathlib import Path

from datafinder_examples_tests.tpch_bih_generator import Generator, generate

SCALE, STEPS, SEED = 0.001, 1000, 1

#: "the current version" in system time: after the last change the history makes
CURRENT_SYS = datetime.datetime(2010, 1, 1)


@functools.lru_cache(maxsize=1)
def generated() -> Generator:
    return generate(SCALE, STEPS, SEED)


@functools.lru_cache(maxsize=1)
def csv_dir() -> Path:
    """The generated data as the CSV files the runner loads."""
    directory = tempfile.mkdtemp(prefix="tpch_bih_")
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    generated().write(Path(directory))
    return Path(directory)
