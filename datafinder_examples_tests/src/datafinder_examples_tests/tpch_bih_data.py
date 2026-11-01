# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportIndexIssue=false, reportReturnType=false
# (pandas is not typed closely enough for frames filtered by boolean masks)
"""The data the TPC-BiH specs run on, and what an oracle needs to work out their expected output from it.

The data is generated (see tpch_bih_generator.py) the first time it is asked for, and the same data is written to the CSV files
the runner loads and held as tables for the oracle, so the expected output of a query is worked out from the rows the finders
read, by code that does not use the finders.
"""
import atexit
import datetime
import functools
import random
import shutil
import tempfile
from pathlib import Path

import pandas as pd

from datafinder_examples_tests.tpch_bih_generator import COLUMNS, Generator, generate

SCALE, STEPS, SEED = 0.001, 1000, 1

#: "the current version", for both dimensions: after the last change the history makes
CURRENT_APP = datetime.date(2010, 1, 1)
CURRENT_SYS = datetime.datetime(2010, 1, 1)

#: the application time of each temporal table, as the prefix of its _begin and _end columns
APP_TIME = {"part": "availability_time", "partsupp": "validity_time", "customer": "visible_time",
            "orders": "active_time", "lineitem": "active_time"}


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


def _text(value):
    """Dates and timestamps as text, which orders as they do and has no limit on the year (9999-12-31)."""
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


@functools.lru_cache(maxsize=None)
def table(name: str) -> pd.DataFrame:
    """Every version of a table, with dates and timestamps as text."""
    rows = [{c: _text(r[c]) for c in COLUMNS[name]} for r in generated().store.versions[name]]
    return pd.DataFrame(rows, columns=COLUMNS[name])


def as_of(name: str, app: str | datetime.date | None = None, sys: str | datetime.datetime | None = None) -> pd.DataFrame:
    """A table at a point in application time and a point in system time; None leaves a dimension open (every version)."""
    df = table(name)
    if sys is not None and "sys_time_begin" in df:
        at = _text(sys)
        df = df[(df.sys_time_begin <= at) & (df.sys_time_end > at)]
    if app is not None and name in APP_TIME:
        at = _text(app)
        prefix = APP_TIME[name]
        df = df[(df[prefix + "_begin"] <= at) & (df[prefix + "_end"] > at)]
    return df


def current(name: str) -> pd.DataFrame:
    return as_of(name, CURRENT_APP, CURRENT_SYS)


def rng(name: str) -> random.Random:
    """A random generator for the parameters of one query, so adding a query does not change another's values."""
    return random.Random(f"tpch-bih-{name}")


def date(text: str) -> datetime.date:
    return datetime.date.fromisoformat(text[:10])


def stamp(text: str) -> datetime.datetime:
    return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
