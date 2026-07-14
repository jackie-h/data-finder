"""Verify DatabricksOutput.to_pandas()/to_numpy() carry correct, nullable per-column types
(from the PyArrow Table returned by cursor.fetchall_arrow()) rather than letting a NULL
silently upcast e.g. an integer column to float64 under plain pandas inference."""
import numpy as np
import pandas as pd
import pyarrow as pa

from datafinder_databricks.databricks_engine import DatabricksOutput

_TABLE = pa.table({
    "Name": pa.array(["Widget", "Gadget"], type=pa.string()),
    # A NULL quantity is what defeats plain pandas inference (upcasts the whole column to
    # float64 and turns 10 into 10.0) unless we use an Arrow-backed nullable dtype.
    "Quantity": pa.array([10, None], type=pa.int32()),
    "Price": pa.array([4.5, 2.0], type=pa.float64()),
    "Active": pa.array([True, False], type=pa.bool_()),
})


class TestDatabricksOutputTypes:

    def test_pandas_dtypes_are_nullable_and_correct(self):
        df = DatabricksOutput(_TABLE).to_pandas()
        assert list(df.columns) == ["Name", "Quantity", "Price", "Active"]
        assert str(df["Name"].dtype) == "string[pyarrow]"
        assert str(df["Quantity"].dtype) == "int32[pyarrow]"
        assert str(df["Price"].dtype) == "double[pyarrow]"
        assert str(df["Active"].dtype) == "bool[pyarrow]"

    def test_integer_value_is_not_coerced_to_float(self):
        df = DatabricksOutput(_TABLE).to_pandas()
        assert df["Quantity"].iloc[0] == 10
        assert isinstance(df["Quantity"].iloc[0], (int, np.integer))
        assert df["Quantity"].iloc[1] is pd.NA

    def test_numpy_cell_types_match_model_types(self):
        row = DatabricksOutput(_TABLE).to_numpy()[0]
        name, quantity, price, active = row
        assert isinstance(name, str) and name == "Widget"
        assert isinstance(quantity, (int, np.integer)) and quantity == 10
        assert isinstance(price, (float, np.floating)) and price == 4.5
        assert isinstance(active, (bool, np.bool_)) and active is True

    def test_numpy_missing_value_is_pd_na(self):
        row = DatabricksOutput(_TABLE).to_numpy()[1]
        assert row[1] is pd.NA
