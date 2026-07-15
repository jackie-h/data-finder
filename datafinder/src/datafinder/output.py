import numpy as np
import pandas as pd


class ToNumpy:

    def to_numpy(self) -> np.ndarray:
        raise NotImplementedError()


class DataFrame(ToNumpy):

    def to_pandas(self) -> pd.DataFrame:
        raise NotImplementedError()


def arrow_table_to_pandas(table) -> pd.DataFrame:
    """Convert a PyArrow Table to a pandas DataFrame using Arrow-backed nullable dtypes
    (pandas.ArrowDtype), so a NULL doesn't silently upcast e.g. an integer column to
    float64 or a boolean column to object the way plain pandas inference would — the
    schema-derived type (Int64, boolean, string, ...) is preserved either way, with
    ``pd.NA`` uniformly representing a missing value."""
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def arrow_table_to_numpy(table) -> np.ndarray:
    """Convert a PyArrow Table directly to a numpy object array with the same nullable-dtype
    semantics as arrow_table_to_pandas() — without allocating a full pandas DataFrame, since
    each column only goes through a lightweight pandas.Series, never a joint multi-column
    frame with its own index/block-manager overhead."""
    if table.num_rows == 0:
        return np.empty((0, table.num_columns), dtype=object)
    columns = [
        table.column(i).to_pandas(types_mapper=pd.ArrowDtype).to_numpy(dtype=object, na_value=pd.NA)
        for i in range(table.num_columns)
    ]
    return np.column_stack(columns)
