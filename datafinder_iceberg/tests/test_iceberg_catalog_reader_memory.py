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
        repo = read_repository_from_catalog(catalog)
        schema_names = {s.name for s in repo.schemas}
        assert schema_names == {"ref_data", "trading"}

    def test_tables_loaded(self, catalog):
        repo = read_repository_from_catalog(catalog)
        by_schema = {s.name: s for s in repo.schemas}
        assert [t.name for t in by_schema["ref_data"].tables] == ["account_master"]
        assert [t.name for t in by_schema["trading"].tables] == ["trades"]

    def test_column_types_mapped(self, catalog):
        repo = read_repository_from_catalog(catalog)
        by_schema = {s.name: s for s in repo.schemas}
        cols = {c.name: c.type for c in by_schema["ref_data"].tables[0].columns}
        assert cols == {"ID": "INT", "ACCT_NAME": "VARCHAR"}

    def test_double_and_boolean_columns(self, catalog):
        repo = read_repository_from_catalog(catalog)
        by_schema = {s.name: s for s in repo.schemas}
        cols = {c.name: c.type for c in by_schema["trading"].tables[0].columns}
        assert cols["price"] == "DOUBLE"
        assert cols["is_settled"] == "BOOLEAN"
        assert cols["in_z"] == "TIMESTAMP"

    def test_name_is_catalog_name(self, catalog):
        repo = read_repository_from_catalog(catalog)
        assert repo.name == "test"


@pytest.fixture
def multi_schema_catalog():
    cat = InMemoryCatalog("test", **{})
    _simple_schema = Schema(NestedField(1, "id", IntegerType()))
    for ns in ("finance_trusted", "finance_raw", "hr_trusted", "hr_raw", "audit"):
        cat.create_namespace(ns)
        cat.create_table(f"{ns}.records", schema=_simple_schema)
    return cat


class TestSchemaFiltering:

    def test_no_filter_loads_all(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog)
        assert {s.name for s in repo.schemas} == {
            "finance_trusted", "finance_raw", "hr_trusted", "hr_raw", "audit"
        }

    def test_include_pattern(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog, include_schemas=["*_trusted"])
        assert {s.name for s in repo.schemas} == {"finance_trusted", "hr_trusted"}

    def test_exclude_pattern(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog, exclude_schemas=["*_raw"])
        assert {s.name for s in repo.schemas} == {"finance_trusted", "hr_trusted", "audit"}

    def test_include_and_exclude(self, multi_schema_catalog):
        repo = read_repository_from_catalog(
            multi_schema_catalog,
            include_schemas=["finance_*"],
            exclude_schemas=["*_raw"],
        )
        assert {s.name for s in repo.schemas} == {"finance_trusted"}

    def test_include_exact_name(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog, include_schemas=["audit"])
        assert {s.name for s in repo.schemas} == {"audit"}

    def test_exclude_all_matches_empty(self, multi_schema_catalog):
        repo = read_repository_from_catalog(
            multi_schema_catalog, exclude_schemas=["finance_*", "hr_*", "audit"]
        )
        assert repo.schemas == []

    def test_include_no_match_empty(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog, include_schemas=["nonexistent_*"])
        assert repo.schemas == []

    def test_multiple_include_patterns(self, multi_schema_catalog):
        repo = read_repository_from_catalog(
            multi_schema_catalog, include_schemas=["finance_trusted", "hr_trusted"]
        )
        assert {s.name for s in repo.schemas} == {"finance_trusted", "hr_trusted"}

    def test_tables_still_loaded_after_filter(self, multi_schema_catalog):
        repo = read_repository_from_catalog(multi_schema_catalog, include_schemas=["*_trusted"])
        for schema in repo.schemas:
            assert len(schema.tables) == 1
            assert schema.tables[0].name == "records"
