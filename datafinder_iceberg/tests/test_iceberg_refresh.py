import pytest
from pyiceberg.catalog.memory import InMemoryCatalog  # type: ignore[import-untyped]
from pyiceberg.schema import Schema  # type: ignore[import-untyped]
from pyiceberg.types import (  # type: ignore[import-untyped]
    NestedField, StringType, IntegerType, DoubleType, TimestampType,
)

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_catalog
from mapping_markdown.refresh import refresh_mapping_content

_EXISTING_MAPPING = """\
# Finance Mapping

## DataStore: finance_db

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

### Schema: trading

#### Table: trades → Trade

| Column | Type      | Key | Property ID   |
|--------|-----------|-----|------------|
| sym    | VARCHAR   |     | symbol     |
| in_z   | TIMESTAMP |     | valid_from |
| out_z  | TIMESTAMP |     | valid_to   |
"""


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
            NestedField(3, "email", StringType()),  # new column
        ),
    )
    cat.create_table(
        "trading.trades",
        schema=Schema(
            NestedField(1, "sym", StringType()),
            # out_z removed
            NestedField(2, "in_z", TimestampType()),
            NestedField(3, "price", DoubleType()),  # new column
        ),
    )
    return cat


class TestIcebergRefresh:

    def setup_method(self, method):
        pass

    def test_existing_property_mappings_preserved(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "id" in result       # Property for ID column
        assert "name" in result     # Property for ACCT_NAME column

    def test_new_column_added(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "email" in result

    def test_new_column_has_empty_property(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        for line in result.splitlines():
            if "email" in line and "|" in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert cells[-1] == ""
                break
        else:
            pytest.fail("email column not found")

    def test_deleted_column_removed(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        col_lines = [
            l for l in result.splitlines()
            if "|" in l and "out_z" in l and "---" not in l and "Column" not in l
        ]
        assert not col_lines, "Deleted column 'out_z' should not appear"

    def test_new_column_in_trades_added(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "price" in result

    def test_table_class_heading_preserved(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "#### Table: account_master → Account" in result
        assert "#### Table: trades → Trade" in result

    def test_schema_headings_preserved(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "### Schema: ref_data" in result
        assert "### Schema: trading" in result

    def test_iceberg_type_reflected(self, catalog):
        repo = read_repository_from_catalog(catalog)
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        for line in result.splitlines():
            if "price" in line and "|" in line and "---" not in line and "Column" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "DOUBLE" in cells
                break
        else:
            pytest.fail("price column not found")


def _key_for_column(result: str, col_name: str) -> str:
    """Extract the Key cell for a named column from a mapping markdown string."""
    for line in result.splitlines():
        if "|" not in line or "---" in line or "Column" in line:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0] == col_name:
            return cells[2] if len(cells) > 2 else ""
    return ""


class TestPrimaryKeyRefresh:

    def test_existing_pk_column_keeps_pk_key(self):
        mapping = """\
# Mapping

## DataStore: db

### Schema: main

#### Table: trades → Trade

| Column | Type    | Key | Property ID |
|--------|---------|-----|----------|
| id     | INT     | PK  | id       |
| sym    | VARCHAR |     | symbol   |
"""
        cat = InMemoryCatalog("test", **{})
        cat.create_namespace("main")
        cat.create_table(
            "main.trades",
            schema=Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "sym", StringType()),
                identifier_field_ids=[1],
            ),
        )
        repo = read_repository_from_catalog(cat)
        result = refresh_mapping_content(mapping, repo)
        assert _key_for_column(result, "id") == "PK"
        assert _key_for_column(result, "sym") == ""

    def test_column_gains_pk_on_refresh(self):
        """A column that was not PK in the mapping becomes PK when Iceberg marks it as identifier."""
        mapping = """\
# Mapping

## DataStore: db

### Schema: main

#### Table: trades → Trade

| Column | Type    | Key | Property ID |
|--------|---------|-----|----------|
| id     | INT     |     | id       |
| sym    | VARCHAR |     | symbol   |
"""
        cat = InMemoryCatalog("test", **{})
        cat.create_namespace("main")
        cat.create_table(
            "main.trades",
            schema=Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "sym", StringType()),
                identifier_field_ids=[1],
            ),
        )
        repo = read_repository_from_catalog(cat)
        result = refresh_mapping_content(mapping, repo)
        assert _key_for_column(result, "id") == "PK"

    def test_fk_column_not_overwritten_by_pk(self):
        """A column marked FK in the mapping is never overwritten by PK detection."""
        mapping = """\
# Mapping

## DataStore: db

### Schema: main

#### Table: trades → Trade

| Column     | Type | Key | Property ID |
|------------|------|-----|----------|
| account_id | INT  | FK  | account  |
"""
        cat = InMemoryCatalog("test", **{})
        cat.create_namespace("main")
        cat.create_table(
            "main.trades",
            schema=Schema(
                NestedField(1, "account_id", IntegerType(), required=True),
                identifier_field_ids=[1],
            ),
        )
        repo = read_repository_from_catalog(cat)
        result = refresh_mapping_content(mapping, repo)
        assert _key_for_column(result, "account_id") == "FK"

    def test_new_column_with_pk_gets_pk_key(self):
        """A brand-new column that is an identifier gets Key=PK in the draft row."""
        mapping = """\
# Mapping

## DataStore: db

### Schema: main

#### Table: trades → Trade

| Column | Type    | Key | Property ID |
|--------|---------|-----|----------|
| sym    | VARCHAR |     | symbol   |
"""
        cat = InMemoryCatalog("test", **{})
        cat.create_namespace("main")
        cat.create_table(
            "main.trades",
            schema=Schema(
                NestedField(1, "sym", StringType()),
                NestedField(2, "id", IntegerType(), required=True),
                identifier_field_ids=[2],
            ),
        )
        repo = read_repository_from_catalog(cat)
        result = refresh_mapping_content(mapping, repo)
        assert _key_for_column(result, "id") == "PK"


_MAPPING_WITH_ONE_SCHEMA = """\
# Mapping

## DataStore: db

### Schema: ref_data

#### Table: accounts → Account

| Column | Type    | Key | Property ID |
|--------|---------|-----|----------|
| id     | INT     | PK  | id       |
| name   | VARCHAR |     | name     |
"""


@pytest.fixture
def catalog_with_new_schema():
    cat = InMemoryCatalog("test", **{})
    cat.create_namespace("ref_data")
    cat.create_namespace("trading")
    cat.create_table(
        "ref_data.accounts",
        schema=Schema(
            NestedField(1, "id", IntegerType()),
            NestedField(2, "name", StringType()),
            NestedField(3, "email", StringType()),   # new column in existing table
        ),
    )
    cat.create_table(
        "ref_data.instruments",                      # new table in existing schema
        schema=Schema(NestedField(1, "sym", StringType())),
    )
    cat.create_table(
        "trading.trades",                            # entirely new schema + table
        schema=Schema(NestedField(1, "price", DoubleType())),
    )
    return cat


class TestExistingOnly:

    def test_existing_only_suppresses_new_schema(self, catalog_with_new_schema):
        repo = read_repository_from_catalog(catalog_with_new_schema)
        result = refresh_mapping_content(_MAPPING_WITH_ONE_SCHEMA, repo, existing_only=True)
        assert "trading" not in result
        assert "trades" not in result

    def test_existing_only_suppresses_new_table_in_existing_schema(self, catalog_with_new_schema):
        repo = read_repository_from_catalog(catalog_with_new_schema)
        result = refresh_mapping_content(_MAPPING_WITH_ONE_SCHEMA, repo, existing_only=True)
        assert "instruments" not in result

    def test_existing_only_still_updates_existing_columns(self, catalog_with_new_schema):
        repo = read_repository_from_catalog(catalog_with_new_schema)
        result = refresh_mapping_content(_MAPPING_WITH_ONE_SCHEMA, repo, existing_only=True)
        assert "email" in result   # new column in existing table is still added

    def test_without_existing_only_new_schema_appears(self, catalog_with_new_schema):
        repo = read_repository_from_catalog(catalog_with_new_schema)
        result = refresh_mapping_content(_MAPPING_WITH_ONE_SCHEMA, repo, existing_only=False)
        assert "trading" in result
        assert "trades" in result

    def test_without_existing_only_new_table_appears(self, catalog_with_new_schema):
        repo = read_repository_from_catalog(catalog_with_new_schema)
        result = refresh_mapping_content(_MAPPING_WITH_ONE_SCHEMA, repo, existing_only=False)
        assert "instruments" in result
