import pytest
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DoubleType, BooleanType, TimestampType,
)

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_catalog


@pytest.fixture
def catalog():
    cat = InMemoryCatalog("test", **{})
    cat.create_namespace("ref_data")
    cat.create_namespace("trading")

    cat.create_table(
        "ref_data.account_master",
        schema=Schema(
            NestedField(1, "ID", IntegerType()),
            NestedField(2, "ACCT_NAME", StringType()),
        ),
    )
    cat.create_table(
        "trading.trades",
        schema=Schema(
            NestedField(1, "sym", StringType()),
            NestedField(2, "price", DoubleType()),
            NestedField(3, "is_settled", BooleanType()),
            NestedField(4, "in_z", TimestampType()),
        ),
    )
    return cat


class TestIcebergCatalogReaderMemory:

    def test_schemas_loaded(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        schema_names = {s.name for s in repo.schemas}
        assert schema_names == {"ref_data", "trading"}

    def test_tables_loaded(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        by_schema = {s.name: s for s in repo.schemas}
        assert [t.name for t in by_schema["ref_data"].tables] == ["account_master"]
        assert [t.name for t in by_schema["trading"].tables] == ["trades"]

    def test_column_types_mapped(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        by_schema = {s.name: s for s in repo.schemas}
        cols = {c.name: c.type for c in by_schema["ref_data"].tables[0].columns}
        assert cols == {"ID": "INT", "ACCT_NAME": "VARCHAR"}

    def test_double_and_boolean_columns(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        by_schema = {s.name: s for s in repo.schemas}
        cols = {c.name: c.type for c in by_schema["trading"].tables[0].columns}
        assert cols["price"] == "DOUBLE"
        assert cols["is_settled"] == "BOOLEAN"
        assert cols["in_z"] == "TIMESTAMP"

    def test_repo_name(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        assert repo.name == "finance_db"
