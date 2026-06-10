import logging
from unittest.mock import MagicMock, patch

import pytest
from pyiceberg.schema import Schema  # type: ignore[import-untyped]
from pyiceberg.types import NestedField, StringType, IntegerType  # type: ignore[import-untyped]

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_iceberg_catalog


def _make_catalog_mock(namespaces, tables_by_namespace, schemas_by_table, name="test_catalog"):
    catalog = MagicMock()
    catalog.name = name
    catalog.list_namespaces.return_value = namespaces
    catalog.list_tables.side_effect = lambda ns: tables_by_namespace.get(tuple(ns), [])
    def _load_table(table_id):
        mock_table = MagicMock()
        mock_table.schema.return_value = schemas_by_table[table_id]
        return mock_table
    catalog.load_table.side_effect = _load_table
    return catalog


def _make_schema(*fields, identifier_field_ids=()):
    id_set = set(identifier_field_ids)
    return Schema(
        *[NestedField(i + 1, name, typ, required=(i + 1) in id_set)
          for i, (name, typ) in enumerate(fields)],
        identifier_field_ids=identifier_field_ids,
    )


@patch("datafinder_iceberg.iceberg_catalog_reader.RestCatalog")
class TestIcebergReader:

    def test_repository_name(self, MockCatalog):
        MockCatalog.return_value = _make_catalog_mock([], {}, {}, name="my_catalog")
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "my_catalog")
        assert repo.name == "my_catalog"

    def test_schemas_and_tables_loaded(self, MockCatalog):
        schema = _make_schema(("id", IntegerType()), ("name", StringType()))
        MockCatalog.return_value = _make_catalog_mock(
            namespaces=[("finance",)],
            tables_by_namespace={("finance",): [("finance", "accounts")]},
            schemas_by_table={("finance", "accounts"): schema},
        )
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")
        assert len(repo.schemas) == 1
        assert repo.schemas[0].name == "finance"
        assert len(repo.schemas[0].tables) == 1
        assert repo.schemas[0].tables[0].name == "accounts"

    def test_columns_mapped(self, MockCatalog):
        schema = _make_schema(("id", IntegerType()), ("name", StringType()))
        MockCatalog.return_value = _make_catalog_mock(
            namespaces=[("finance",)],
            tables_by_namespace={("finance",): [("finance", "accounts")]},
            schemas_by_table={("finance", "accounts"): schema},
        )
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")
        cols = {c.name: c.type for c in repo.schemas[0].tables[0].columns}
        assert cols == {"id": "INT", "name": "VARCHAR"}

    def test_identifier_fields_marked_as_primary_key(self, MockCatalog):
        # field_id=1 for "id", field_id=2 for "name"; mark field 1 as identifier
        schema = _make_schema(("id", IntegerType()), ("name", StringType()),
                              identifier_field_ids=(1,))
        MockCatalog.return_value = _make_catalog_mock(
            namespaces=[("finance",)],
            tables_by_namespace={("finance",): [("finance", "accounts")]},
            schemas_by_table={("finance", "accounts"): schema},
        )
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")
        cols = {c.name: c.primary_key for c in repo.schemas[0].tables[0].columns}
        assert cols == {"id": True, "name": False}

    def test_no_identifier_fields_all_false(self, MockCatalog):
        schema = _make_schema(("id", IntegerType()), ("name", StringType()))
        MockCatalog.return_value = _make_catalog_mock(
            namespaces=[("finance",)],
            tables_by_namespace={("finance",): [("finance", "accounts")]},
            schemas_by_table={("finance", "accounts"): schema},
        )
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")
        assert all(not c.primary_key for c in repo.schemas[0].tables[0].columns)

    def test_composite_primary_key(self, MockCatalog):
        schema = _make_schema(
            ("fund_id", IntegerType()), ("account_id", IntegerType()), ("name", StringType()),
            identifier_field_ids=(1, 2),
        )
        MockCatalog.return_value = _make_catalog_mock(
            namespaces=[("finance",)],
            tables_by_namespace={("finance",): [("finance", "accounts")]},
            schemas_by_table={("finance", "accounts"): schema},
        )
        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")
        cols = {c.name: c.primary_key for c in repo.schemas[0].tables[0].columns}
        assert cols == {"fund_id": True, "account_id": True, "name": False}

    def test_fail_on_error_true_raises(self, MockCatalog):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("finance",)]
        catalog.list_tables.return_value = [("finance", "not_iceberg")]
        catalog.load_table.side_effect = Exception("Not an Iceberg table")
        MockCatalog.return_value = catalog

        with pytest.raises(Exception, match="Not an Iceberg table"):
            read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog", fail_on_error=True)

    def test_fail_on_error_false_skips_table(self, MockCatalog):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("finance",)]
        catalog.list_tables.return_value = [("finance", "not_iceberg")]
        catalog.load_table.side_effect = Exception("Not an Iceberg table")
        MockCatalog.return_value = catalog

        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog", fail_on_error=False)
        assert repo.schemas[0].tables == []

    def test_fail_on_error_false_logs_warning(self, MockCatalog, caplog):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("finance",)]
        catalog.list_tables.return_value = [("finance", "not_iceberg")]
        catalog.load_table.side_effect = Exception("Not an Iceberg table")
        MockCatalog.return_value = catalog

        with caplog.at_level(logging.WARNING, logger="datafinder_iceberg.iceberg_catalog_reader"):
            read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog", fail_on_error=False)

        assert "not_iceberg" in caplog.text
        assert "Not an Iceberg table" in caplog.text

    def test_fail_on_error_defaults_to_true(self, MockCatalog):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("finance",)]
        catalog.list_tables.return_value = [("finance", "not_iceberg")]
        catalog.load_table.side_effect = Exception("Not an Iceberg table")
        MockCatalog.return_value = catalog

        with pytest.raises(Exception):
            read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog")

    def test_skips_bad_table_but_loads_good_ones(self, MockCatalog):
        good_schema = _make_schema(("id", IntegerType()))
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("finance",)]
        catalog.list_tables.return_value = [
            ("finance", "not_iceberg"),
            ("finance", "accounts"),
        ]
        def _load(table_id):
            if table_id[-1] == "not_iceberg":
                raise Exception("Not an Iceberg table")
            mock = MagicMock()
            mock.schema.return_value = good_schema
            return mock
        catalog.load_table.side_effect = _load
        MockCatalog.return_value = catalog

        repo = read_repository_from_iceberg_catalog("https://catalog.example.com", "test_catalog", fail_on_error=False)
        table_names = [t.name for t in repo.schemas[0].tables]
        assert table_names == ["accounts"]
