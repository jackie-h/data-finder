import contextlib
import datetime
import threading
from typing import Callable, Iterator

import numpy as np
import pandas as pd


class ToNumpy:

    def to_numpy(self) -> np.ndarray:
        raise NotImplementedError()


class DataFrame(ToNumpy):

    def to_pandas(self) -> pd.DataFrame:
        raise NotImplementedError()


@contextlib.contextmanager
def interrupt_after(timeout_ms: int, interrupt: Callable[[], None]) -> Iterator[None]:
    """Call ``interrupt`` once ``timeout_ms`` is up, and again every few milliseconds until the block ends. A single call is not
    enough: an interrupt that comes before the database has started the query (while a library is still building it) does
    nothing, and the query then runs to the end."""
    finished = threading.Event()

    def watch() -> None:
        if not finished.wait(timeout_ms / 1000):
            while True:
                interrupt()
                if finished.wait(0.005):
                    return

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        yield
    finally:
        finished.set()
        watcher.join()


def _intervals_as_durations(table):
    """A column of intervals as one of durations, which is what a ``timedelta`` is. A database gives an interval as months,
    days and nanoseconds, and pandas has no dtype for that; months have no fixed length, so an interval of months is refused."""
    import pyarrow as pa
    for i, field in enumerate(table.schema):
        if pa.types.is_interval(field.type):
            values = []
            for value in table.column(i).to_pylist():
                if value is not None and value.months:
                    raise ValueError(f"An interval of months ({value.months}) is not a length of time: it has no fixed number of days")
                values.append(None if value is None else datetime.timedelta(days=value.days, microseconds=value.nanoseconds // 1000))
            table = table.set_column(i, pa.field(field.name, pa.duration("us")), pa.array(values, pa.duration("us")))
    return table


def arrow_table_to_pandas(table) -> pd.DataFrame:
    """Convert a PyArrow Table to a pandas DataFrame using Arrow-backed nullable dtypes
    (pandas.ArrowDtype), so a NULL doesn't silently upcast e.g. an integer column to
    float64 or a boolean column to object the way plain pandas inference would — the
    schema-derived type (Int64, boolean, string, ...) is preserved either way, with
    ``pd.NA`` uniformly representing a missing value."""
    return _intervals_as_durations(table).to_pandas(types_mapper=pd.ArrowDtype)


def arrow_table_to_numpy(table) -> np.ndarray:
    """Convert a PyArrow Table directly to a numpy object array with the same nullable-dtype
    semantics as arrow_table_to_pandas() — without allocating a full pandas DataFrame, since
    each column only goes through a lightweight pandas.Series, never a joint multi-column
    frame with its own index/block-manager overhead."""
    table = _intervals_as_durations(table)
    if table.num_rows == 0:
        return np.empty((0, table.num_columns), dtype=object)
    columns = [
        table.column(i).to_pandas(types_mapper=pd.ArrowDtype).to_numpy(dtype=object, na_value=pd.NA)
        for i in range(table.num_columns)
    ]
    return np.column_stack(columns)
