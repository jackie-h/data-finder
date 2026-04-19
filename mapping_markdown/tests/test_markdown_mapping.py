import os
import tempfile

from mapping_markdown.markdown_mapping import load, loads, save, to_markdown
from model_markdown.markdown_model import load as load_model
from model.m3 import Class
from model.relational import Repository, Schema, Table, Column
from model.mapping import ProcessingDateMilestonesPropertyMapping
from model.relational_mapping import RelationalPropertyMapping, Join

FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping.md")
MODEL_FILE = os.path.join(os.path.dirname(__file__), "finance.md")


def _build_repository() -> Repository:
    repo = Repository("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trading)
    return repo


class TestMarkdownMappingLoad:

    def setup_method(self):
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.repo)
        self.by_class = {rcm.clazz.name: rcm for rcm in self.mapping.mappings}

    def test_mapping_title(self):
        assert self.mapping.name == "Finance Mapping"

    def test_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Instrument", "Trade"}

    def test_account_column_mappings(self):
        rcm = self.by_class["Account"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        assert by_prop["id"].target.name == "ID"
        assert by_prop["name"].target.name == "ACCT_NAME"

    def test_property_mapping_types(self):
        rcm = self.by_class["Account"]
        for pm in rcm.property_mappings:
            assert isinstance(pm, RelationalPropertyMapping)
            assert isinstance(pm.target, Column)

    def test_classes_resolved_from_model_file(self):
        account = self.by_class["Account"].clazz
        assert isinstance(account, Class)
        assert account.package.name == "finance"

    def test_milestoning_schemes_loaded_onto_repository(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        assert set(schemes.keys()) == {"bitemporal", "processing_only"}

    def test_processing_only_scheme_columns(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        s = schemes["processing_only"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date is None

    def test_bitemporal_scheme_columns(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        s = schemes["bitemporal"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date == "business_date"

    def test_table_milestoning_scheme_on_mapping(self):
        assert self.by_class["Account"].milestone_mapping is None
        assert isinstance(self.by_class["Instrument"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)
        assert isinstance(self.by_class["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)

    def test_milestone_mapping_columns(self):
        mm = self.by_class["Trade"].milestone_mapping
        assert mm._in.target.name == "in_z"
        assert mm._out.target.name == "out_z"

    def test_join_mapping_for_non_primitive_property(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        join = by_prop["account"].target
        assert isinstance(join, Join)
        assert join.lhs.name == "account_id"
        assert join.rhs.name == "ID"

    def test_generated_association_uses_model_name(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "#### Association: TradeAccount" in content

    def test_join_target_table(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        assert by_prop["account"].target.rhs.table.name == "account_master"


class TestMarkdownMappingSave:

    def setup_method(self):
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.repo)
        self.packages = load_model(MODEL_FILE)

    def test_roundtrip(self):
        content = to_markdown("Finance Mapping", self.mapping, "finance.md")
        repo2 = _build_repository()
        mapping2 = loads(content, self.packages, repo2)
        by_class2 = {rcm.clazz.name: rcm for rcm in mapping2.mappings}
        assert set(by_class2.keys()) == {"Account", "Instrument", "Trade"}
        assert isinstance(by_class2["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)
        assert by_class2["Account"].milestone_mapping is None
        by_prop2 = {pm.property.name: pm for pm in by_class2["Trade"].property_mappings}
        assert isinstance(by_prop2["account"].target, Join)

    def test_save_and_reload(self):
        fixture_dir = os.path.dirname(FIXTURE)
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w", dir=fixture_dir) as f:
            temp_path = f.name
        try:
            save(temp_path, "Finance Mapping", self.mapping, "finance.md")
            repo2 = _build_repository()
            mapping2 = load(temp_path, repo2)
            assert len(mapping2.mappings) == 3
        finally:
            os.unlink(temp_path)

    def test_generated_markdown_has_model_reference(self):
        content = to_markdown("Finance Mapping", self.mapping, "finance.md")
        assert "## Model: finance.md" in content

    def test_generated_markdown_has_repository(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "## Repository: finance_db" in content

    def test_generated_markdown_has_schemas(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "### Schema: ref_data" in content
        assert "### Schema: trading" in content

    def test_generated_markdown_has_milestoning_schemes(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "bitemporal" in content
        assert "processing_only" in content

    def test_generated_markdown_has_association(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "#### Association: TradeAccount" in content
