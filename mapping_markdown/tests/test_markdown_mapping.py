import os
import tempfile

from mapping_markdown.markdown_mapping import (
    load, loads, save, to_markdown,
    RepositoryMapping, SchemaMapping, TableMapping,
    ColumnMapping, MilestoningOverride, AssociationMapping,
)

FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping.md")


class TestMarkdownMappingLoad:

    def setup_method(self):
        self.repos = load(FIXTURE)
        self.repo = self.repos[0]
        self.schemas = {s.schema: s for s in self.repo.schema_mappings}
        self.ref_data = self.schemas["ref_data"]
        self.trading = self.schemas["trading"]
        self.tables_ref = {t.table: t for t in self.ref_data.table_mappings}
        self.tables_trading = {t.table: t for t in self.trading.table_mappings}

    def test_repository_name(self):
        assert len(self.repos) == 1
        assert self.repo.name == "finance_db"

    def test_milestoning_schemes(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        assert set(schemes.keys()) == {"bitemporal", "processing_only"}

    def test_bitemporal_scheme_columns(self):
        s = {s.name: s for s in self.repo.milestoning_schemes}["bitemporal"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date == "business_date"

    def test_processing_only_scheme_columns(self):
        s = {s.name: s for s in self.repo.milestoning_schemes}["processing_only"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date is None

    def test_schema_names(self):
        assert set(self.schemas.keys()) == {"ref_data", "trading"}

    def test_ref_data_tables(self):
        assert set(self.tables_ref.keys()) == {"account_master", "price"}

    def test_trading_tables(self):
        assert set(self.tables_trading.keys()) == {"trades", "contractualposition"}

    def test_table_class_mapping(self):
        assert self.tables_ref["account_master"].cls == "Account"
        assert self.tables_ref["price"].cls == "Instrument"
        assert self.tables_trading["trades"].cls == "Trade"

    def test_table_milestoning_scheme(self):
        assert self.tables_ref["account_master"].milestoning_scheme is None
        assert self.tables_ref["price"].milestoning_scheme == "processing_only"
        assert self.tables_trading["trades"].milestoning_scheme == "processing_only"
        assert self.tables_trading["contractualposition"].milestoning_scheme == "bitemporal"

    def test_column_mappings(self):
        cols = {cm.column: cm.property for cm in self.tables_ref["account_master"].column_mappings}
        assert cols["ID"] == "id"
        assert cols["ACCT_NAME"] == "name"

    def test_trade_column_mappings(self):
        cols = {cm.column: cm.property for cm in self.tables_trading["trades"].column_mappings}
        assert cols["sym"] == "symbol"
        assert cols["account_id"] == "account"

    def test_milestoning_overrides(self):
        overrides = self.tables_trading["contractualposition"].milestoning_overrides
        assert len(overrides) == 1
        assert overrides[0].scheme == "bitemporal"
        assert overrides[0].milestoning == "business_date"
        assert overrides[0].column == "DATE"

    def test_no_overrides_when_absent(self):
        assert self.tables_trading["trades"].milestoning_overrides == []

    def test_association_mapping(self):
        assocs = self.trading.association_mappings
        assert len(assocs) == 1
        assert assocs[0].name == "TradeAccount"
        assert assocs[0].source_column == "account_id"
        assert assocs[0].target_table == "account_master"
        assert assocs[0].target_column == "ID"


class TestMarkdownMappingSave:

    def setup_method(self):
        self.repos = load(FIXTURE)

    def test_roundtrip(self):
        content = to_markdown("Finance Mapping", self.repos)
        repos2 = loads(content)
        assert repos2[0].name == "finance_db"
        schemas2 = {s.schema: s for s in repos2[0].schema_mappings}
        assert set(schemas2.keys()) == {"ref_data", "trading"}
        tables2 = {t.table: t for t in schemas2["trading"].table_mappings}
        assert tables2["trades"].cls == "Trade"
        assert tables2["trades"].milestoning_scheme == "processing_only"
        assert len(schemas2["trading"].association_mappings) == 1

    def test_save_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w") as f:
            path = f.name
        try:
            save(path, "Finance Mapping", self.repos)
            repos2 = load(path)
            assert len(repos2[0].schema_mappings) == 2
        finally:
            os.unlink(path)

    def test_generated_markdown_has_repository(self):
        content = to_markdown("Finance Mapping", self.repos)
        assert "## Repository: finance_db" in content

    def test_generated_markdown_has_schemas(self):
        content = to_markdown("Finance Mapping", self.repos)
        assert "### Schema: ref_data" in content
        assert "### Schema: trading" in content

    def test_generated_markdown_has_milestoning_schemes(self):
        content = to_markdown("Finance Mapping", self.repos)
        assert "bitemporal" in content
        assert "processing_only" in content

    def test_generated_markdown_has_association(self):
        content = to_markdown("Finance Mapping", self.repos)
        assert "#### Association: TradeAccount" in content
