from unittest.mock import MagicMock, patch

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, DoubleType, DateType

from datafinder_iceberg.iceberg_loader import load_schema_from_catalog


def _make_schema():
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "symbol", StringType()),
        NestedField(3, "price", DoubleType()),
        NestedField(4, "trade_date", DateType()),
    )


class TestIcebergCatalogLoader:

    @patch("datafinder_iceberg.iceberg_loader.RestCatalog")
    def test_loads_table_from_catalog(self, MockCatalog):
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.schema.return_value = _make_schema()
        MockCatalog.return_value.load_table.return_value = mock_iceberg_table

        table = load_schema_from_catalog(
            catalog_uri="https://catalog.example.com",
            namespace="finance",
            table_name="trades",
        )

        MockCatalog.assert_called_once_with("catalog", uri="https://catalog.example.com")
        MockCatalog.return_value.load_table.assert_called_once_with(("finance", "trades"))
        assert table.name == "trades"
        assert len(table.columns) == 4
        assert table.columns[0].type == "INT"
        assert table.columns[1].type == "VARCHAR"
        assert table.columns[2].type == "DOUBLE"
        assert table.columns[3].type == "DATE"

    @patch("datafinder_iceberg.iceberg_loader.RestCatalog")
    def test_credentials_passed_to_catalog(self, MockCatalog):
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.schema.return_value = _make_schema()
        MockCatalog.return_value.load_table.return_value = mock_iceberg_table

        credentials = {"token": "my-secret-token"}
        load_schema_from_catalog(
            catalog_uri="https://catalog.example.com",
            namespace="finance",
            table_name="trades",
            credentials=credentials,
        )

        MockCatalog.assert_called_once_with(
            "catalog",
            uri="https://catalog.example.com",
            token="my-secret-token",
        )

    @patch("datafinder_iceberg.iceberg_loader.RestCatalog")
    def test_no_credentials_by_default(self, MockCatalog):
        mock_iceberg_table = MagicMock()
        mock_iceberg_table.schema.return_value = _make_schema()
        MockCatalog.return_value.load_table.return_value = mock_iceberg_table

        load_schema_from_catalog("https://catalog.example.com", "finance", "trades")

        MockCatalog.assert_called_once_with("catalog", uri="https://catalog.example.com")
