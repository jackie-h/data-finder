import pytest
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DoubleType, TimestampType,
)

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_catalog
from mapping_markdown.refresh import refresh_mapping_content

_EXISTING_MAPPING = """\
# Finance Mapping

## Repository: finance_db

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

### Schema: trading

#### Table: trades → Trade

| Column | Type      | Key | Property   |
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
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "id" in result       # Property for ID column
        assert "name" in result     # Property for ACCT_NAME column

    def test_new_column_added(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "email" in result

    def test_new_column_has_empty_property(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        for line in result.splitlines():
            if "email" in line and "|" in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert cells[-1] == ""
                break
        else:
            pytest.fail("email column not found")

    def test_deleted_column_removed(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        col_lines = [
            l for l in result.splitlines()
            if "|" in l and "out_z" in l and "---" not in l and "Column" not in l
        ]
        assert not col_lines, "Deleted column 'out_z' should not appear"

    def test_new_column_in_trades_added(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "price" in result

    def test_table_class_heading_preserved(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "#### Table: account_master → Account" in result
        assert "#### Table: trades → Trade" in result

    def test_schema_headings_preserved(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        assert "### Schema: ref_data" in result
        assert "### Schema: trading" in result

    def test_iceberg_type_reflected(self, catalog):
        repo = read_repository_from_catalog(catalog, repo_name="finance_db")
        result = refresh_mapping_content(_EXISTING_MAPPING, repo)
        for line in result.splitlines():
            if "price" in line and "|" in line and "---" not in line and "Column" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "DOUBLE" in cells
                break
        else:
            pytest.fail("price column not found")
