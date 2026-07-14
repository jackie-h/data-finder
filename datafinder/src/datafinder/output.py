import numpy as np
import pandas as pd


class ToNumpy:

    def to_numpy(self) -> np.ndarray:
        raise NotImplementedError()


class DataFrame(ToNumpy):

    def to_pandas(self) -> pd.DataFrame:
        raise NotImplementedError()


def _dtype_for_column(col) -> str | None:
    """Best-effort pandas dtype for a display column, derived from its declared model type.

    Only used for backends whose driver doesn't already hand back a correctly-typed frame
    (e.g. Databricks' DB-API cursor, where a NULL silently upcasts an integer column to
    float64 under plain pandas inference). Returns None when there's no reliable mapping,
    so the caller falls back to pandas' own inference rather than risk a wrong coercion.
    """
    from datafinder.attribute import Attribute
    from datafinder.typed_attributes import (
        StringAttribute, IntegerAttribute, DoubleAttribute, BooleanAttribute,
        DateAttribute, DateTimeAttribute,
    )
    from model.relational import (
        CountAllOperation, AggregateOperation, AggregateOperator,
        WindowFunctionOperation, WindowFunction, DateExtractOperation,
    )

    if isinstance(col, CountAllOperation):
        return 'Int64'
    if isinstance(col, AggregateOperation) and col.operator == AggregateOperator.COUNT:
        return 'Int64'
    if isinstance(col, WindowFunctionOperation) and col.function in (
        WindowFunction.ROW_NUMBER, WindowFunction.RANK, WindowFunction.DENSE_RANK, WindowFunction.NTILE,
    ):
        return 'Int64'
    if isinstance(col, DateExtractOperation):
        return 'Int64'
    if isinstance(col, BooleanAttribute):
        return 'boolean'
    if isinstance(col, IntegerAttribute):
        return 'Int64'
    if isinstance(col, DoubleAttribute):
        return 'float64'
    if isinstance(col, StringAttribute):
        return 'object'
    if isinstance(col, (DateAttribute, DateTimeAttribute)):
        return 'datetime64[ns]'
    return None


def build_typed_dataframe(rows, column_names: list[str], display_columns: list | None = None) -> pd.DataFrame:
    """Build a pandas DataFrame with the given column names, coercing each column to its
    declared model dtype when one can be determined (leaving pandas' own inference in place
    otherwise, or if the coercion itself fails against the actual data returned)."""
    df = pd.DataFrame(rows, columns=column_names)  # type: ignore[arg-type]
    if not display_columns:
        return df
    for name, col in zip(column_names, display_columns):
        dtype = _dtype_for_column(col)
        if dtype is None:
            continue
        try:
            df[name] = df[name].astype(dtype)
        except (TypeError, ValueError):
            pass
    return df

