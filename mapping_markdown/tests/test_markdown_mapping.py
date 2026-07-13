import os
import tempfile

import pytest
from mapping_markdown.markdown_mapping import load, loads, save, to_markdown
from model_markdown.markdown_model import load as load_model
from model.m3 import Class
from model.relational import Database, Schema, Table, Column
from model.mapping import ProcessingDateMilestonesPropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping, \
    BiTemporalMilestonePropertyMapping
from model.relational_mapping import RelationalPropertyMapping, Join

FIXTURE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples", "finance_mapping.md"))
MODEL_FILE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples", "finance.md"))
TRADE_MODEL_FILE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples", "finance_trade.md"))


def _build_repository() -> Database:
    repo = Database("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"), Column("is_settled", "BOOLEAN"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trading)
    Table("contractualposition", [Column("DATE", "DATE"), Column("QUANTITY", "DOUBLE"),
                                  Column("NPV", "DOUBLE"), Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trading)
    return repo


class TestMarkdownMappingLoad:

    def setup_method(self):
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.repo)
        self.by_class = {rcm.clazz.name: rcm for rcm in self.mapping.mappings}

    def test_mapping_title(self):
        assert self.mapping.name == "Finance Mapping"

    def test_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Instrument", "Trade", "ContractualPosition"}

    def test_account_column_mappings(self):
        rcm = self.by_class["Account"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
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
        assert account.package is not None
        assert account.package.name == "finance.reference_data"

    def test_milestoning_schemes_loaded_onto_repository(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        assert set(schemes.keys()) == {"bitemporal", "processing_only", "business_date_processing"}

    def test_processing_only_scheme_columns(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        s = schemes["processing_only"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date is None
        assert s.business_date_from is None

    def test_business_date_processing_scheme_columns(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        s = schemes["business_date_processing"]
        assert s.business_date == "DATE"
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date_from is None

    def test_bitemporal_scheme_columns(self):
        schemes = {s.name: s for s in self.repo.milestoning_schemes}
        s = schemes["bitemporal"]
        assert s.processing_start == "in_z"
        assert s.processing_end == "out_z"
        assert s.business_date_from == "DATE_FROM"
        assert s.business_date_to == "DATE_TO"

    def test_table_milestoning_scheme_on_mapping(self):
        assert self.by_class["Account"].milestone_mapping is None
        assert isinstance(self.by_class["Instrument"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)
        assert isinstance(self.by_class["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)
        assert isinstance(self.by_class["ContractualPosition"].milestone_mapping, BusinessDateAndProcessingMilestonePropertyMapping)

    def test_business_date_processing_milestone_mapping_columns(self):
        mm = self.by_class["ContractualPosition"].milestone_mapping
        assert isinstance(mm, BusinessDateAndProcessingMilestonePropertyMapping)
        assert mm._date.target.name == "DATE"
        assert mm._in.target.name == "in_z"
        assert mm._out.target.name == "out_z"

    def test_milestone_mapping_columns(self):
        mm = self.by_class["Trade"].milestone_mapping
        assert isinstance(mm, ProcessingDateMilestonesPropertyMapping)
        assert mm._in.target.name == "in_z"
        assert mm._out.target.name == "out_z"

    def test_instrument_milestoning_columns_not_in_model(self):
        instrument_cls = self.by_class["Instrument"].clazz
        assert "validFrom" not in instrument_cls.properties
        assert "validTo" not in instrument_cls.properties

    def test_instrument_milestone_mapping_built_from_synthetic_properties(self):
        mm = self.by_class["Instrument"].milestone_mapping
        assert isinstance(mm, ProcessingDateMilestonesPropertyMapping)
        assert mm._in.target.name == "in_z"
        assert mm._out.target.name == "out_z"

    def test_instrument_synthetic_milestoning_properties_in_mappings(self):
        rcm = self.by_class["Instrument"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        assert "validFrom" in by_prop
        assert "validTo" in by_prop
        assert by_prop["validFrom"].target.name == "in_z"
        assert by_prop["validTo"].target.name == "out_z"

    def test_primary_key_set_on_column(self):
        rcm = self.by_class["Account"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        assert by_prop["id"].target.primary_key is True
        assert by_prop["name"].target.primary_key is False

    def test_instrument_primary_key(self):
        rcm = self.by_class["Instrument"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        assert by_prop["symbol"].target.primary_key is True

    def test_foreign_key_on_table(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        trades_table = by_prop["symbol"].target.table
        assert len(trades_table.foreign_keys) == 1
        fk = trades_table.foreign_keys[0]
        assert fk.column.name == "account_id"
        assert fk.references.name == "ID"
        assert fk.references.table.name == "account_master"

    def test_non_fk_column_not_in_foreign_keys(self):
        rcm = self.by_class["Account"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        account_table = by_prop["id"].target.table
        assert len(account_table.foreign_keys) == 0

    def test_join_mapping_for_non_primitive_property(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        join = by_prop["account"].target
        assert isinstance(join, Join)
        assert join.lhs.name == "account_id"
        assert join.rhs.name == "ID"

    def test_generated_association_uses_model_name(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "#### Association: TradeAccount" in content

    def test_join_target_table(self):
        rcm = self.by_class["Trade"]
        by_prop = {pm.property.id: pm for pm in rcm.property_mappings}
        assert by_prop["account"].target.rhs.table.name == "account_master"


class TestMarkdownMappingSave:

    def setup_method(self):
        self.repo = _build_repository()
        self.mapping = load(FIXTURE, self.repo)
        packages1 = load_model(MODEL_FILE)
        known = {c.name: c for pkg in packages1 for c in pkg.children if isinstance(c, Class)}
        packages2 = load_model(TRADE_MODEL_FILE, known_classes=known)
        self.packages = packages1 + packages2

    def test_roundtrip(self):
        content = to_markdown("Finance Mapping", self.mapping, ["finance.md", "finance_trade.md"])
        repo2 = _build_repository()
        mapping2 = loads(content, self.packages, repo2)
        by_class2 = {rcm.clazz.name: rcm for rcm in mapping2.mappings}
        assert set(by_class2.keys()) == {"Account", "Instrument", "Trade", "ContractualPosition"}
        assert isinstance(by_class2["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)
        assert by_class2["Account"].milestone_mapping is None
        by_prop2 = {pm.property.id: pm for pm in by_class2["Trade"].property_mappings}
        assert isinstance(by_prop2["account"].target, Join)

    def test_roundtrip_preserves_primary_key(self):
        content = to_markdown("Finance Mapping", self.mapping, ["finance.md", "finance_trade.md"])
        repo2 = _build_repository()
        mapping2 = loads(content, self.packages, repo2)
        by_class2 = {rcm.clazz.name: rcm for rcm in mapping2.mappings}
        by_prop2 = {pm.property.id: pm for pm in by_class2["Account"].property_mappings}
        assert by_prop2["id"].target.primary_key is True
        assert by_prop2["name"].target.primary_key is False

    def test_roundtrip_preserves_foreign_key(self):
        content = to_markdown("Finance Mapping", self.mapping, ["finance.md", "finance_trade.md"])
        repo2 = _build_repository()
        mapping2 = loads(content, self.packages, repo2)
        by_class2 = {rcm.clazz.name: rcm for rcm in mapping2.mappings}
        by_prop2 = {pm.property.id: pm for pm in by_class2["Trade"].property_mappings}
        trades_table = by_prop2["symbol"].target.table
        assert len(trades_table.foreign_keys) == 1
        assert trades_table.foreign_keys[0].column.name == "account_id"

    def test_generated_markdown_has_pk_column(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "| ID        | INT     | PK  | id          |" in content

    def test_generated_markdown_has_fk_column(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "FK" in content

    def test_save_and_reload(self):
        fixture_dir = os.path.dirname(FIXTURE)
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w", dir=fixture_dir) as f:
            temp_path = f.name
        try:
            save(temp_path, "Finance Mapping", self.mapping, ["finance.md", "finance_trade.md"])
            repo2 = _build_repository()
            mapping2 = load(temp_path, repo2)
            assert len(mapping2.mappings) == 4
        finally:
            os.unlink(temp_path)

    def test_generated_markdown_has_model_reference(self):
        content = to_markdown("Finance Mapping", self.mapping, ["finance.md", "finance_trade.md"])
        assert "## Model: finance.md" in content
        assert "## Model: finance_trade.md" in content

    def test_generated_markdown_has_repository(self):
        content = to_markdown("Finance Mapping", self.mapping)
        assert "## DataStore: finance_db" in content

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


SPLIT_FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping_split.md")


class TestMarkdownMappingSplitLoad:
    """Load via a parent file that references per-schema files."""

    def setup_method(self):
        self.mapping = load(SPLIT_FIXTURE)
        self.by_class = {rcm.clazz.name: rcm for rcm in self.mapping.mappings}

    def test_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Instrument", "Trade"}

    def test_repository_built_from_parent(self):
        table = self.by_class["Account"].property_mappings[0].target.table
        assert table.schema.datastore.name == "finance_db"

    def test_schemas_from_included_files(self):
        account_table = self.by_class["Account"].property_mappings[0].target.table
        trade_table = self.by_class["Trade"].property_mappings[0].target.table
        assert account_table.schema.name == "ref_data"
        assert trade_table.schema.name == "trading"

    def test_milestoning_resolved(self):
        assert isinstance(self.by_class["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)

    def test_join_resolved(self):
        by_prop = {pm.property.id: pm for pm in self.by_class["Trade"].property_mappings}
        assert isinstance(by_prop["account"].target, Join)


class TestMarkdownMappingLoadNoRepository:

    def setup_method(self):
        self.mapping = load(FIXTURE)
        self.by_class = {rcm.clazz.name: rcm for rcm in self.mapping.mappings}

    def test_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Instrument", "Trade", "ContractualPosition"}

    def test_repository_built_from_markdown(self):
        table = self.by_class["Account"].property_mappings[0].target.table
        assert table.schema.datastore.name == "finance_db"

    def test_schemas_built_from_markdown(self):
        account_table = self.by_class["Account"].property_mappings[0].target.table
        instrument_table = self.by_class["Instrument"].property_mappings[0].target.table
        assert account_table.schema.name == "ref_data"
        assert instrument_table.schema.name == "ref_data"

    def test_columns_built_from_markdown(self):
        by_prop = {pm.property.id: pm for pm in self.by_class["Account"].property_mappings}
        assert by_prop["id"].target.name == "ID"
        assert by_prop["id"].target.type == "INT"

    def test_milestoning_resolved_from_markdown(self):
        assert isinstance(self.by_class["Trade"].milestone_mapping, ProcessingDateMilestonesPropertyMapping)

    def test_join_resolved_from_markdown(self):
        by_prop = {pm.property.id: pm for pm in self.by_class["Trade"].property_mappings}
        assert isinstance(by_prop["account"].target, Join)


class TestAssociationWithoutFkInTableSection:
    """Association mapping must work even when the FK column is not listed as a property
    in the Table section. Previously, omitting account_id from the Trade table properties
    caused the association to be silently dropped."""

    _MAPPING = """\
# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

### Schema: trading

#### Table: trades → Trade

| Column     | Type    | Key | Property ID   |
|------------|---------|-----|------------|
| sym        | VARCHAR |     | symbol     |
| price      | DOUBLE  |     | price      |
| is_settled | BOOLEAN |     | isSettled  |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
"""

    def setup_method(self):
        repo = Database("finance_db", "duckdb://test.db")
        ref_data = Schema("ref_data", repo)
        trading = Schema("trading", repo)
        Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
        Table(
            "trades",
            [
                Column("sym", "VARCHAR"),
                Column("price", "DOUBLE"),
                Column("is_settled", "BOOLEAN"),
                Column("account_id", "INT"),
            ],
            trading,
        )
        known = {}
        packages1 = load_model(MODEL_FILE, known_classes=known)
        known.update({c.name: c for pkg in packages1 for c in pkg.children if isinstance(c, Class)})
        packages2 = load_model(TRADE_MODEL_FILE, known_classes=known)
        packages = packages1 + packages2
        mapping = loads(self._MAPPING, packages, repo)
        self.by_class = {rcm.clazz.name: rcm for rcm in mapping.mappings}

    def test_trade_class_mapped(self):
        assert "Trade" in self.by_class

    def test_association_property_resolved(self):
        """account property must appear in Trade's property mappings despite account_id being absent
        from the Table section."""
        by_prop = {pm.property.id: pm for pm in self.by_class["Trade"].property_mappings}
        assert "account" in by_prop, (
            "account navigation property missing — association was not resolved "
            "when FK column was absent from the Table property list"
        )

    def test_association_join_target(self):
        by_prop = {pm.property.id: pm for pm in self.by_class["Trade"].property_mappings}
        join = by_prop["account"].target
        assert isinstance(join, Join)
        assert join.lhs.name == "account_id"
        assert join.rhs.name == "ID"

    def test_foreign_key_added(self):
        by_prop = {pm.property.id: pm for pm in self.by_class["Trade"].property_mappings}
        trades_table = by_prop["symbol"].target.table
        assert len(trades_table.foreign_keys) == 1
        fk = trades_table.foreign_keys[0]
        assert fk.column.name == "account_id"
        assert fk.references.name == "ID"


class TestInfiniteDatetimeMarkdownParsing:

    _MAPPING_WITH_INFINITE = """\
# Finance Mapping

## DataStore: finance_db (Database)

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime         |
|-----------------|------------------|----------------|---------------|--------------------|------------------|---------------------------|
| processing_only | in_z             | out_z          |               |                    |                  | 9999-12-31 23:59:59       |
| open_ended      | in_z             | out_z          |               |                    |                  |                           |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|----------|
| in_z   | TIMESTAMP |     | validFrom  |
| out_z  | TIMESTAMP |     | validTo    |
"""

    def setup_method(self):
        from mapping_markdown.markdown_mapping import _md_parser, _build_repository_from_content, _loads_from_nodes
        from markdown_it.tree import SyntaxTreeNode  # type: ignore
        root = SyntaxTreeNode(_md_parser.parse(self._MAPPING_WITH_INFINITE))
        nodes = root.children
        repo = _build_repository_from_content(nodes)
        assert repo is not None
        # milestoning schemes are populated inside _loads_from_nodes
        _loads_from_nodes(nodes, packages=[], repository=repo)
        self.repo = repo
        self.schemes = {s.name: s for s in self.repo.milestoning_schemes}

    def test_infinite_datetime_parsed_when_set(self):
        assert self.schemes["processing_only"].infinite_datetime == "9999-12-31 23:59:59"

    def test_infinite_datetime_is_none_when_blank(self):
        assert self.schemes["open_ended"].infinite_datetime is None

    def test_infinite_datetime_flows_to_milestone_mapping(self):
        from mapping_markdown.markdown_mapping import _md_parser, _build_repository_from_content, _loads_from_nodes
        from markdown_it.tree import SyntaxTreeNode  # type: ignore
        from model.mapping import ProcessingDateMilestonesPropertyMapping
        root = SyntaxTreeNode(_md_parser.parse(self._MAPPING_WITH_INFINITE))
        nodes = root.children
        repo = _build_repository_from_content(nodes)
        assert repo is not None
        mapping = _loads_from_nodes(nodes, packages=[], repository=repo)
        schemes = {s.name: s for s in repo.milestoning_schemes}
        assert schemes["processing_only"].infinite_datetime == "9999-12-31 23:59:59"
        assert schemes["open_ended"].infinite_datetime is None


class TestDuplicateClassMappingValidation:

    _DUPLICATE_MAPPING = """\
# Duplicate Mapping

## DataStore: test_db (Database)

### Schema: hr

#### Table: accounts → Account

| Column | Type    | Key | Property ID |
|--------|---------|-----|-------------|
| id     | INT     | PK  | id          |
| name   | VARCHAR |     | name        |

#### Table: accounts_copy → Account

| Column | Type    | Key | Property ID |
|--------|---------|-----|-------------|
| id     | INT     | PK  | id          |
| name   | VARCHAR |     | name        |
"""

    def test_duplicate_class_in_markdown_raises(self):
        import pytest
        from model.m3 import Package, Class, Property, Integer, String
        from model.relational import Database, Schema, Table, Column

        pkg = Package("test")
        account_cls = Class("Account", [
            Property("Id", "id", Integer),
            Property("Name", "name", String),
        ], pkg)

        db = Database("test_db", "duckdb://test.db")
        schema = Schema("hr", db)
        Table("accounts", [Column("id", "INT"), Column("name", "VARCHAR")], schema)
        Table("accounts_copy", [Column("id", "INT"), Column("name", "VARCHAR")], schema)

        with pytest.raises(ValueError, match="Class 'Account' is mapped more than once"):
            loads(self._DUPLICATE_MAPPING, packages=[pkg], datastore=db)


class TestDuplicateAssociationMapping:

    _MAPPING = """\
# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| ACCT_NAME | VARCHAR |     | name        |

### Schema: trading

#### Table: trades → Trade

| Column     | Type    | Key | Property ID |
|------------|---------|-----|-------------|
| sym        | VARCHAR |     | symbol      |
| price      | DOUBLE  |     | price       |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
"""

    def setup_method(self):
        self._repo = Database("finance_db", "duckdb://test.db")
        ref_data = Schema("ref_data", self._repo)
        trading = Schema("trading", self._repo)
        Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
        Table(
            "trades",
            [
                Column("sym", "VARCHAR"),
                Column("price", "DOUBLE"),
                Column("account_id", "INT"),
            ],
            trading,
        )
        known = {}
        packages1 = load_model(MODEL_FILE, known_classes=known)
        known.update({c.name: c for pkg in packages1 for c in pkg.children if isinstance(c, Class)})
        packages2 = load_model(TRADE_MODEL_FILE, known_classes=known)
        self._packages = packages1 + packages2

    def test_duplicate_association_raises(self):
        with pytest.raises(ValueError, match="already mapped"):
            loads(self._MAPPING, self._packages, self._repo)


class TestAssociationBeforeTableRaises:

    _MAPPING = """\
# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| ACCT_NAME | VARCHAR |     | name        |

### Schema: trading

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |

#### Table: trades → Trade

| Column | Type    | Key | Property ID |
|--------|---------|-----|-------------|
| sym    | VARCHAR |     | symbol      |
| price  | DOUBLE  |     | price       |
"""

    def setup_method(self):
        self._repo = Database("finance_db", "duckdb://test.db")
        ref_data = Schema("ref_data", self._repo)
        trading = Schema("trading", self._repo)
        Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
        Table(
            "trades",
            [Column("sym", "VARCHAR"), Column("price", "DOUBLE"), Column("account_id", "INT")],
            trading,
        )
        known = {}
        packages1 = load_model(MODEL_FILE, known_classes=known)
        known.update({c.name: c for pkg in packages1 for c in pkg.children if isinstance(c, Class)})
        packages2 = load_model(TRADE_MODEL_FILE, known_classes=known)
        self._packages = packages1 + packages2

    def test_association_before_table_raises(self):
        with pytest.raises(ValueError, match="no Table section defined before"):
            loads(self._MAPPING, self._packages, self._repo)


class TestColumnMappingHeaderValidation:

    def _mapping_with_headers(self, header_row: str, separator_row: str) -> str:
        return f"""\
# Test Mapping

## DataStore: test_db (Database)

### Schema: hr

#### Table: accounts → Account

{header_row}
{separator_row}
| id     | INT | PK  | id   |
"""

    def _make_repo_and_pkg(self):
        from model.m3 import Package, Class, Property, Integer
        from model.relational import Database, Schema, Table, Column
        pkg = Package("test")
        Class("Account", [Property("Id", "id", Integer)], pkg)
        db = Database("test_db", "duckdb://test.db")
        Table("accounts", [Column("id", "INT")], Schema("hr", db))
        return pkg, db

    def test_property_column_raises(self):
        content = self._mapping_with_headers(
            "| Column | Type | Key | Property |",
            "|--------|------|-----|----------|",
        )
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="unexpected headers.*'Property'.*expected.*'Property ID'"):
            loads(content, packages=[pkg], datastore=db)

    def test_wrong_order_raises(self):
        content = self._mapping_with_headers(
            "| Type | Column | Key | Property ID |",
            "|------|--------|-----|-------------|",
        )
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="unexpected headers"):
            loads(content, packages=[pkg], datastore=db)

    def test_missing_property_id_raises(self):
        content = self._mapping_with_headers(
            "| Column | Type | Key |",
            "|--------|------|-----|",
        )
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="unexpected headers"):
            loads(content, packages=[pkg], datastore=db)

    def test_correct_headers_accepted(self):
        content = self._mapping_with_headers(
            "| Column | Type | Key | Property ID |",
            "|--------|------|-----|-------------|",
        )
        pkg, db = self._make_repo_and_pkg()
        mapping = loads(content, packages=[pkg], datastore=db)
        assert len(mapping.mappings) == 1


class TestMilestonedWithMissingColumns:

    _BASE = """\
# Test Mapping

## DataStore: test_db (Database)

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to |
|-----------------|------------------|----------------|---------------|--------------------|------------------|
| processing_only | in_z             | out_z          |               |                    |                  |
| biz_date        | in_z             | out_z          | biz_date      |                    |                  |

### Schema: hr

"""

    def _make_repo_and_pkg(self):
        from model.m3 import Package, Class, Property, String
        pkg = Package("test")
        Class("Trade", [
            Property("Symbol", "symbol", String),
            Property("Valid From", "validFrom", String),
            Property("Valid To", "validTo", String),
        ], pkg)
        db = Database("test_db", "duckdb://test.db")
        Table("trades", [
            Column("sym", "VARCHAR"),
            Column("in_z", "TIMESTAMP"),
            Column("out_z", "TIMESTAMP"),
        ], Schema("hr", db))
        return pkg, db

    def test_missing_processing_start_raises(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: processing_only)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| sym    | VARCHAR   |     | symbol      |
| out_z  | TIMESTAMP |     | validTo     |
"""
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="missing required column 'in_z'"):
            loads(content, packages=[pkg], datastore=db)

    def test_missing_processing_end_raises(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: processing_only)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| sym    | VARCHAR   |     | symbol      |
| in_z   | TIMESTAMP |     | validFrom   |
"""
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="missing required column 'out_z'"):
            loads(content, packages=[pkg], datastore=db)

    def test_missing_both_processing_columns_raises(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: processing_only)

| Column | Type    | Key | Property ID |
|--------|---------|-----|-------------|
| sym    | VARCHAR |     | symbol      |
"""
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="missing required column"):
            loads(content, packages=[pkg], datastore=db)

    def test_unknown_scheme_raises(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: no_such_scheme)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| sym    | VARCHAR   |     | symbol      |
| in_z   | TIMESTAMP |     | validFrom   |
| out_z  | TIMESTAMP |     | validTo     |
"""
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="milestoning scheme 'no_such_scheme' not found"):
            loads(content, packages=[pkg], datastore=db)

    def test_error_message_includes_table_and_class(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: processing_only)

| Column | Type    | Key | Property ID |
|--------|---------|-----|-------------|
| sym    | VARCHAR |     | symbol      |
"""
        pkg, db = self._make_repo_and_pkg()
        with pytest.raises(ValueError, match="Table 'trades'.*'Trade'"):
            loads(content, packages=[pkg], datastore=db)

    def test_all_columns_present_succeeds(self):
        content = self._BASE + """\
#### Table: trades → Trade (milestoning: processing_only)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| sym    | VARCHAR   |     | symbol      |
| in_z   | TIMESTAMP |     | validFrom   |
| out_z  | TIMESTAMP |     | validTo     |
"""
        pkg, db = self._make_repo_and_pkg()
        mapping = loads(content, packages=[pkg], datastore=db)
        assert isinstance(mapping.mappings[0].milestone_mapping, ProcessingDateMilestonesPropertyMapping)


class TestEmbeddedMapping:

    _MAPPING = """\
# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| ACCT_NAME | VARCHAR |     | name        |

### Schema: trading

#### Table: trades → Trade

| Column     | Type    | Key | Property ID  |
|------------|---------|-----|--------------|
| sym        | VARCHAR |     | symbol       |
| price      | DOUBLE  |     | price        |
| account_id | INT     | FK  | account      |
| acct_name  | VARCHAR |     | account.name |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|----------------|----------------|---------------|
| account_id     | account_master | ID            |
"""

    def _build_repo(self) -> Database:
        repo = Database("finance_db", "duckdb://test.db")
        ref_data = Schema("ref_data", repo)
        trading = Schema("trading", repo)
        Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
        Table(
            "trades",
            [Column("sym", "VARCHAR"), Column("price", "DOUBLE"), Column("account_id", "INT"),
             Column("acct_name", "VARCHAR")],
            trading,
        )
        return repo

    def _build_packages(self):
        known = {}
        packages1 = load_model(MODEL_FILE, known_classes=known)
        known.update({c.name: c for pkg in packages1 for c in pkg.children if isinstance(c, Class)})
        packages2 = load_model(TRADE_MODEL_FILE, known_classes=known)
        return packages1 + packages2

    def test_dotted_property_id_builds_embedded_set_mapping(self):
        packages = self._build_packages()
        mapping = loads(self._MAPPING, packages, self._build_repo())
        rcm = next(m for m in mapping.mappings if m.clazz.name == "Trade")
        pm = next(pm for pm in rcm.property_mappings if pm.property.id == "account")
        assert isinstance(pm.target, Join)
        assert pm.target.embedded is not None
        assert pm.target.embedded.clazz.name == "Account"
        leaf = pm.target.embedded.property_mappings[0]
        assert leaf.property.id == "name"
        assert isinstance(leaf.target, Column)
        assert leaf.target.name == "acct_name"

    def test_unresolvable_nav_segment_raises(self):
        content = self._MAPPING.replace("account.name", "bogus.name")
        packages = self._build_packages()
        with pytest.raises(ValueError, match="could not resolve navigation property 'bogus'"):
            loads(content, packages, self._build_repo())

    def test_unresolvable_leaf_property_raises(self):
        content = self._MAPPING.replace("account.name", "account.bogus")
        packages = self._build_packages()
        with pytest.raises(ValueError, match="property 'bogus' not found"):
            loads(content, packages, self._build_repo())

    def test_embedded_without_association_raises(self):
        content = self._MAPPING.replace(
            """#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|----------------|----------------|---------------|
| account_id     | account_master | ID            |
""",
            "",
        )
        packages = self._build_packages()
        with pytest.raises(ValueError, match="have no matching Association section"):
            loads(content, packages, self._build_repo())

    def test_roundtrip_preserves_embedded_mapping(self):
        packages = self._build_packages()
        mapping = loads(self._MAPPING, packages, self._build_repo())
        content = to_markdown("Finance Mapping", mapping, model_paths=["finance.md", "finance_trade.md"])
        assert "| acct_name" in content and "account.name" in content

        mapping2 = loads(content, packages, self._build_repo())
        rcm = next(m for m in mapping2.mappings if m.clazz.name == "Trade")
        pm = next(pm for pm in rcm.property_mappings if pm.property.id == "account")
        assert pm.target.embedded is not None
        leaf = pm.target.embedded.property_mappings[0]
        assert leaf.property.id == "name" and leaf.target.name == "acct_name"

    def test_two_hop_dotted_path_builds_nested_embedded_set_mapping(self):
        from model.m3 import Package, Class, Property, Association, String, Integer, ONE_TO_ONE, ZERO_TO_ONE

        pkg = Package("test")
        branch = Class("Branch", [Property("City", "city", String)], pkg)
        account = Class("Account", [Property("Id", "id", Integer)], pkg)
        trade = Class("Trade", [Property("Symbol", "symbol", String)], pkg)
        Association("AccountBranch", "Account", ONE_TO_ONE, "Account", "account",
                    "Branch", ZERO_TO_ONE, "Branch", "branch", pkg)
        Association("TradeAccount", "Trade", ONE_TO_ONE, "Trade", "trade",
                    "Account", ZERO_TO_ONE, "Account", "account", pkg)

        repo = Database("test_db", "duckdb://test.db")
        schema = Schema("s", repo)
        Table("account_master", [Column("ID", "INT")], schema)
        Table(
            "trades",
            [Column("sym", "VARCHAR"), Column("account_id", "INT"), Column("branch_city", "VARCHAR")],
            schema,
        )

        content = """\
# Test Mapping

## DataStore: test_db (Database)

### Schema: s

#### Table: account_master → Account

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID     | INT  | PK  | id          |

#### Table: trades → Trade

| Column      | Type    | Key | Property ID          |
|-------------|---------|-----|-----------------------|
| sym         | VARCHAR |     | symbol                |
| account_id  | INT     | FK  | account               |
| branch_city | VARCHAR |     | account.branch.city   |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|----------------|----------------|---------------|
| account_id     | account_master | ID            |
"""
        mapping = loads(content, [pkg], repo)
        rcm = next(m for m in mapping.mappings if m.clazz.name == "Trade")
        pm = next(pm for pm in rcm.property_mappings if pm.property.id == "account")
        assert pm.target.embedded is not None
        branch_pm = pm.target.embedded.property_mappings[0]
        assert branch_pm.property.id == "branch"
        from model.relational_mapping import EmbeddedSetMapping
        assert isinstance(branch_pm.target, EmbeddedSetMapping)
        city_pm = branch_pm.target.property_mappings[0]
        assert city_pm.property.id == "city"
        assert city_pm.target.name == "branch_city"
