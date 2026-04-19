import os
import tempfile

from mapping_markdown.markdown_mapping import load, loads, save, to_markdown
from model.m3 import Package, Class, Property, String, Integer, Float
from model.mapping import Mapping
from model.relational import Repository, Schema, Table, Column
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping.md")


def _build_packages() -> list:
    pkg = Package("finance")
    account = Class("Account", [Property("id", Integer), Property("name", String)], pkg)
    instrument = Class("Instrument", [Property("symbol", String), Property("price", Float)], pkg)
    Class("Trade", [
        Property("id", Integer),
        Property("sym", String),
        Property("price", Float),
        Property("account", account),
    ], pkg)
    return [pkg]


def _build_repository() -> Repository:
    repo = Repository("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("id", "INT"), Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trading)
    return repo


class TestMarkdownMappingLoad:

    def setup_method(self):
        self.packages = _build_packages()
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.packages, self.repo)
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
        assert self.by_class["Account"].milestoning_scheme is None
        assert self.by_class["Instrument"].milestoning_scheme == "processing_only"
        assert self.by_class["Trade"].milestoning_scheme == "processing_only"

    def test_join_mapping_for_non_primitive_property(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        join = by_prop["account"].target
        assert isinstance(join, Join)
        assert join.lhs.name == "account_id"
        assert join.rhs.name == "ID"

    def test_join_association_name(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        assert by_prop["account"].target.name == "TradeAccount"

    def test_join_target_table(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.name: pm for pm in rcm.property_mappings}
        assert by_prop["account"].target.rhs.table.name == "account_master"


class TestMarkdownMappingSave:

    def setup_method(self):
        self.packages = _build_packages()
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.packages, self.repo)

    def test_roundtrip(self):
        content = to_markdown("Finance Mapping", self.mapping)
        packages2 = _build_packages()
        repo2 = _build_repository()
        mapping2 = loads(content, packages2, repo2)
        by_class2 = {rcm.clazz.name: rcm for rcm in mapping2.mappings}
        assert set(by_class2.keys()) == {"Account", "Instrument", "Trade"}
        assert by_class2["Trade"].milestoning_scheme == "processing_only"
        assert by_class2["Account"].milestoning_scheme is None
        by_prop2 = {pm.property.name: pm for pm in by_class2["Trade"].property_mappings}
        assert isinstance(by_prop2["account"].target, Join)

    def test_save_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w") as f:
            path = f.name
        try:
            save(path, "Finance Mapping", self.mapping)
            packages2 = _build_packages()
            repo2 = _build_repository()
            mapping2 = load(path, packages2, repo2)
            assert len(mapping2.mappings) == 3
        finally:
            os.unlink(path)

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
