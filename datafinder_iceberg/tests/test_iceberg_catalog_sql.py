import os
import shutil
import sys
import tempfile

import pytest
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType,
)

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_catalog
from mapping_markdown.markdown_mapping import load
from datafinder_generator.generator import generate

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_mapping.md")
)

_CATALOG_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "finance_catalog_mapping.md")
)

_FINDER_MODULES = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.account_finder_base",
    "finance.reference_data.instrument_finder",
    "finance.reference_data.instrument_finder_base",
    "finance.trade.trade_finder",
    "finance.trade.trade_finder_base",
]


def _build_iceberg_catalog(catalog_name: str) -> InMemoryCatalog:
    cat = InMemoryCatalog(catalog_name, **{})
    cat.create_namespace("ref_data")
    cat.create_namespace("trading")
    cat.create_table("ref_data.account_master", schema=IcebergSchema(
        NestedField(1, "ID", IntegerType()),
        NestedField(2, "ACCT_NAME", StringType()),
    ))
    cat.create_table("ref_data.price", schema=IcebergSchema(
        NestedField(1, "SYM", StringType()),
        NestedField(2, "PRICE", DoubleType()),
        NestedField(3, "in_z", TimestampType()),
        NestedField(4, "out_z", TimestampType()),
    ))
    cat.create_table("trading.trades", schema=IcebergSchema(
        NestedField(1, "sym", StringType()),
        NestedField(2, "price", DoubleType()),
        NestedField(3, "is_settled", BooleanType()),
        NestedField(4, "account_id", IntegerType()),
        NestedField(5, "in_z", TimestampType()),
        NestedField(6, "out_z", TimestampType()),
    ))
    cat.create_table("trading.contractualposition", schema=IcebergSchema(
        NestedField(1, "DATE", DateType()),
        NestedField(2, "QUANTITY", DoubleType()),
        NestedField(3, "NPV", DoubleType()),
        NestedField(4, "in_z", TimestampType()),
        NestedField(5, "out_z", TimestampType()),
    ))
    return cat


class TestDataCatalogQualifiedName:
    """Unit tests: DataCatalog carries catalog name into qualified_name."""

    def test_datastore_is_datacatalog(self):
        cat = _build_iceberg_catalog("my_catalog")
        repo = read_repository_from_catalog(cat)
        from model.relational import DataCatalog
        assert isinstance(repo, DataCatalog)

    def test_catalog_name_set(self):
        cat = _build_iceberg_catalog("my_catalog")
        repo = read_repository_from_catalog(cat)
        assert repo.name == "my_catalog"

    def test_schema_datastore_is_datacatalog(self):
        cat = _build_iceberg_catalog("my_catalog")
        repo = read_repository_from_catalog(cat)
        from model.relational import DataCatalog
        by_schema = {s.name: s for s in repo.schemas}
        assert isinstance(by_schema["ref_data"].datastore, DataCatalog)

    def test_table_qualified_name_includes_catalog(self):
        cat = _build_iceberg_catalog("my_catalog")
        repo = read_repository_from_catalog(cat)
        by_schema = {s.name: s for s in repo.schemas}
        table = next(t for t in by_schema["ref_data"].tables if t.name == "account_master")
        assert table.qualified_name == "my_catalog.ref_data.account_master"


@pytest.fixture(scope="module")
def finders():
    """Load mapping.md via _build_repository_from_content, then compare against catalog."""
    cat = _build_iceberg_catalog("my_catalog")
    catalog_repo = read_repository_from_catalog(cat)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    # load() without a repo argument uses _build_repository_from_content to parse the
    # markdown structure, then we pass the catalog repo so its DataCatalog tables
    # (with qualified_name = catalog.schema.table) replace the plain markdown-built ones.
    mapping = load(_MAPPING_FILE, catalog_repo)
    generate(mapping, temp_dir)

    from finance.reference_data.account_finder import AccountFinder
    from finance.reference_data.instrument_finder import InstrumentFinder
    from finance.trade.trade_finder import TradeFinder

    yield {"Account": AccountFinder(), "Instrument": InstrumentFinder(), "Trade": TradeFinder()}

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestIcebergCatalogPrefixInSQL:
    """E2E: catalog prefix flows from DataCatalog through mapping into generated SQL."""

    def test_account_sql_contains_catalog_prefix(self, finders):
        account = finders["Account"]
        sql = account.find_all(
            None, None, [account.id_(), account.name()],
        ).to_sql()
        assert "my_catalog.ref_data.account_master" in sql

    def test_instrument_sql_contains_catalog_prefix(self, finders):
        instrument = finders["Instrument"]
        sql = instrument.find_all(
            None, "2024-01-01 00:00:00", [instrument.symbol(), instrument.price()],
        ).to_sql()
        assert "my_catalog.ref_data.price" in sql

    def test_trade_sql_contains_catalog_prefix(self, finders):
        trade = finders["Trade"]
        sql = trade.find_all(
            None, "2024-01-01 00:00:00", [trade.symbol(), trade.price()],
        ).to_sql()
        assert "my_catalog.trading.trades" in sql


_CATALOG_MAPPING_MODULES = [
    "finance", "finance.reference_data", "finance.trade",
    "finance.reference_data.account_finder",
    "finance.reference_data.account_finder_base",
    "finance.trade.trade_finder",
    "finance.trade.trade_finder_base",
]


@pytest.fixture(scope="module")
def catalog_mapping_finders():
    """Load finance_catalog_mapping.md with no external repo.

    _build_repository_from_content parses the (Catalog: my_catalog) heading and
    creates a DataCatalog directly — no iceberg connection needed.
    """
    temp_dir = tempfile.mkdtemp()
    for mod in _CATALOG_MAPPING_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_CATALOG_MAPPING_FILE)
    generate(mapping, temp_dir)

    from finance.reference_data.account_finder import AccountFinder
    from finance.trade.trade_finder import TradeFinder

    yield {"Account": AccountFinder(), "Trade": TradeFinder()}

    sys.path.remove(temp_dir)
    for mod in _CATALOG_MAPPING_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestCatalogMappingFileSQL:
    """E2E: (Catalog: ...) in mapping.md flows through _build_repository_from_content into SQL."""

    def test_datastore_is_datacatalog(self):
        from model.relational import DataCatalog
        from mapping_markdown.markdown_mapping import load as md_load
        mapping = md_load(_CATALOG_MAPPING_FILE)
        from mapping_markdown.markdown_mapping import _primary_table
        table = _primary_table(mapping.mappings[0])
        assert isinstance(table.schema.datastore, DataCatalog)
        assert table.schema.datastore.name == "my_catalog"

    def test_account_sql_contains_catalog_prefix(self, catalog_mapping_finders):
        account = catalog_mapping_finders["Account"]
        sql = account.find_all(
            None, None, [account.id_(), account.name()],
        ).to_sql()
        assert "my_catalog.ref_data.account_master" in sql

    def test_trade_sql_contains_catalog_prefix(self, catalog_mapping_finders):
        trade = catalog_mapping_finders["Trade"]
        sql = trade.find_all(
            None, "2024-01-01 00:00:00", [trade.symbol(), trade.price()],
        ).to_sql()
        assert "my_catalog.trading.trades" in sql
