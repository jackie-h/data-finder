"""Verify DatabricksOutput.to_pandas()/to_numpy() coerce each column to its declared model
type, rather than letting a NULL silently upcast e.g. an integer column to float64 under
plain pandas inference (databricks-sql-connector has no equivalent of DuckDB's own .df())."""
import numpy as np
import pandas as pd

from datafinder.typed_attributes import StringAttribute, IntegerAttribute, DoubleAttribute, BooleanAttribute
from datafinder_databricks.databricks_engine import DatabricksOutput


def _columns():
    return [
        StringAttribute("Name", "name", "VARCHAR", "widgets"),
        IntegerAttribute("Quantity", "quantity", "INT", "widgets"),
        DoubleAttribute("Price", "price", "DOUBLE", "widgets"),
        BooleanAttribute("Active", "active", "BOOLEAN", "widgets"),
    ]


_ROWS = [
    ("Widget", 10, 4.5, True),
    # A NULL quantity is what defeats plain pandas inference (upcasts the whole column to
    # float64 and turns 10 into 10.0) unless we coerce to a nullable integer dtype.
    ("Gadget", None, 2.0, False),
]
_NAMES = ["Name", "Quantity", "Price", "Active"]


class TestDatabricksOutputTypes:

    def test_pandas_dtypes_match_model_types_even_with_nulls(self):
        df = DatabricksOutput(_ROWS, _NAMES, _columns()).to_pandas()
        assert df["Name"].dtype == object
        assert str(df["Quantity"].dtype) == "Int64"
        assert np.issubdtype(df["Price"].dtype, np.floating)  # type: ignore[arg-type]
        assert str(df["Active"].dtype) == "boolean"

    def test_integer_value_is_not_coerced_to_float(self):
        df = DatabricksOutput(_ROWS, _NAMES, _columns()).to_pandas()
        assert df["Quantity"].iloc[0] == 10
        assert isinstance(df["Quantity"].iloc[0], (int, np.integer))
        assert df["Quantity"].iloc[1] is pd.NA

    def test_numpy_cell_types_match_model_types(self):
        row = DatabricksOutput(_ROWS, _NAMES, _columns()).to_numpy()[0]
        name, quantity, price, active = row
        assert isinstance(name, str) and name == "Widget"
        assert isinstance(quantity, (int, np.integer)) and quantity == 10
        assert isinstance(price, (float, np.floating)) and price == 4.5
        assert isinstance(active, (bool, np.bool_)) and active is True

    def test_without_display_columns_falls_back_to_plain_inference(self):
        # No display_columns supplied -> no coercion attempted; still produces a valid frame.
        df = DatabricksOutput(_ROWS, _NAMES).to_pandas()
        assert list(df.columns) == _NAMES
