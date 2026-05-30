import os
import shutil
import sys
import tempfile

import pytest
from mapping_markdown.markdown_mapping import load
from model.m3 import Property, String, Class, Package, Association, Multiplicity, _name_to_camel_id
from datafinder_generator.generator import to_python_name, generate, _class_package, _ensure_package_dirs, \
    to_snake_case, _mapping_to_class_name, _mapping_to_filename


def _prop(label: str, id: str = None) -> Property:
    return Property(label, id or _name_to_camel_id(label), String)


class TestToPythonName:

    def test_single_word(self):
        assert to_python_name(_prop("Price")) == "price"

    def test_multi_word_label(self):
        assert to_python_name(_prop("Valid From")) == "valid_from"

    def test_multi_word_lowercase(self):
        assert to_python_name(_prop("Is Settled")) == "is_settled"

    def test_keyword_clash_from(self):
        assert to_python_name(_prop("From")) == "from_"

    def test_keyword_clash_type(self):
        assert to_python_name(_prop("Type")) == "type_"

    def test_keyword_clash_class(self):
        assert to_python_name(_prop("Class")) == "class_"

    def test_keyword_clash_import(self):
        assert to_python_name(_prop("Import")) == "import_"

    def test_builtin_clash_id(self):
        assert to_python_name(_prop("Id", "id")) == "id_"

    def test_builtin_clash_list(self):
        assert to_python_name(_prop("List")) == "list_"

    def test_no_clash_normal(self):
        assert to_python_name(_prop("Account")) == "account"

    def test_uses_label_not_id(self):
        prop = Property("Valid From", "validFrom", String)
        assert to_python_name(prop) == "valid_from"


class TestToSnakeCase:

    def test_single_word(self):
        assert to_snake_case("Account") == "account"

    def test_pascal_case(self):
        assert to_snake_case("ContractualPosition") == "contractual_position"

    def test_already_lower(self):
        assert to_snake_case("trade") == "trade"


class TestMappingNaming:

    def test_class_name_from_spaced_name(self):
        assert _mapping_to_class_name("Finance Mapping") == "FinanceMappingContext"

    def test_class_name_from_numbered_name(self):
        assert _mapping_to_class_name("Test Mapping 1") == "TestMapping1Context"

    def test_class_name_from_single_word(self):
        assert _mapping_to_class_name("OrgMapping") == "OrgMappingContext"

    def test_filename_from_spaced_name(self):
        assert _mapping_to_filename("Finance Mapping") == "finance_mapping_context.py"

    def test_filename_from_numbered_name(self):
        assert _mapping_to_filename("Test Mapping 1") == "test_mapping_1_context.py"

    def test_filename_from_single_word(self):
        assert _mapping_to_filename("OrgMapping") == "orgmapping_context.py"


_FINANCE_MAPPING = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_mapping.md")
)


class TestClassPackage:

    def test_dotted_package_name_returned(self):
        from model.m3 import Class, Package
        pkg = Package("finance.reference_data")
        cls = Class("Account", [], pkg)
        assert _class_package(cls) == "finance.reference_data"

    def test_simple_name_returns_empty(self):
        from model.m3 import Class, Package
        pkg = Package("finance")
        cls = Class("Account", [], pkg)
        assert _class_package(cls) == ""

    def test_no_package_returns_empty(self):
        from model.m3 import Class
        cls = Class("Account", [], None)
        assert _class_package(cls) == ""


class TestEnsurePackageDirs:

    def test_creates_nested_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            leaf = _ensure_package_dirs(tmp, "org.finance.trade")
            assert os.path.isdir(os.path.join(tmp, "org", "finance", "trade"))
            assert leaf == os.path.join(tmp, "org", "finance", "trade")

    def test_creates_init_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            _ensure_package_dirs(tmp, "org.finance.trade")
            assert os.path.isfile(os.path.join(tmp, "org", "__init__.py"))
            assert os.path.isfile(os.path.join(tmp, "org", "finance", "__init__.py"))
            assert os.path.isfile(os.path.join(tmp, "org", "finance", "trade", "__init__.py"))

    def test_idempotent_on_second_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            _ensure_package_dirs(tmp, "a.b")
            _ensure_package_dirs(tmp, "a.b")  # should not raise


class TestGeneratePackageStructure:

    def setup_method(self):
        from model.relational import Database, Schema, Table, Column
        self.tmp = tempfile.mkdtemp()
        repo = Database("finance_db", "duckdb://test.db")
        ref_data = Schema("ref_data", repo)
        trading = Schema("trading", repo)
        Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
        Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                        Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
        Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                         Column("is_settled", "BOOLEAN"), Column("account_id", "INT"),
                         Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], trading)
        self.mapping = load(_FINANCE_MAPPING, repo)

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
        for mod in list(sys.modules.keys()):
            if mod.startswith("finance"):
                sys.modules.pop(mod, None)

    def test_finders_written_to_package_directories(self):
        generate(self.mapping, self.tmp)
        assert os.path.isfile(os.path.join(self.tmp, "finance", "reference_data", "account_finder.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "reference_data", "instrument_finder.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "trade", "trade_finder.py"))

    def test_base_files_written_to_package_directories(self):
        generate(self.mapping, self.tmp)
        assert os.path.isfile(os.path.join(self.tmp, "finance", "reference_data", "account_finder_base.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "reference_data", "instrument_finder_base.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "trade", "trade_finder_base.py"))

    def test_context_file_written_to_output_root(self):
        generate(self.mapping, self.tmp)
        assert os.path.isfile(os.path.join(self.tmp, "finance_mapping_context.py"))

    def test_init_files_created_at_every_level(self):
        generate(self.mapping, self.tmp)
        assert os.path.isfile(os.path.join(self.tmp, "finance", "__init__.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "reference_data", "__init__.py"))
        assert os.path.isfile(os.path.join(self.tmp, "finance", "trade", "__init__.py"))

    def test_generated_finder_uses_package_import_for_related_class(self):
        generate(self.mapping, self.tmp)
        trade_finder_path = os.path.join(self.tmp, "finance", "trade", "trade_finder.py")
        content = open(trade_finder_path).read()
        assert "from finance.reference_data.account_finder import AccountRelatedFinder" in content

    def test_generated_finder_imports_own_base(self):
        generate(self.mapping, self.tmp)
        trade_finder_path = os.path.join(self.tmp, "finance", "trade", "trade_finder.py")
        content = open(trade_finder_path).read()
        assert "from finance.trade.trade_finder_base import TradeFinderBase, TradeRelatedFinderBase" in content

    def test_generated_finder_is_importable(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance.trade.trade_finder import TradeFinder
            assert TradeFinder is not None
        finally:
            sys.path.remove(self.tmp)

    def test_finder_is_instance_based(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance.reference_data.account_finder import AccountFinder
            finder = AccountFinder()
            assert finder.id_() is not None
            assert finder.name() is not None
        finally:
            sys.path.remove(self.tmp)

    def test_reverse_association_on_account_finder(self):
        generate(self.mapping, self.tmp)
        account_finder_path = os.path.join(self.tmp, "finance", "reference_data", "account_finder.py")
        content = open(account_finder_path).read()
        assert "def trades(" in content

    def test_reverse_association_uses_lazy_import(self):
        generate(self.mapping, self.tmp)
        account_finder_path = os.path.join(self.tmp, "finance", "reference_data", "account_finder.py")
        content = open(account_finder_path).read()
        assert "from finance.trade.trade_finder import TradeRelatedFinder" in content

    def test_reverse_association_is_callable(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance.reference_data.account_finder import AccountFinder
            result = AccountFinder().trades()
            assert result is not None
        finally:
            sys.path.remove(self.tmp)

    def test_context_is_importable(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance_mapping_context import FinanceMappingContext
            ctx = FinanceMappingContext()
            assert ctx is not None
        finally:
            sys.path.remove(self.tmp)

    def test_context_exposes_all_finders(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance_mapping_context import FinanceMappingContext
            from finance.reference_data.account_finder_base import AccountFinderBase
            from finance.trade.trade_finder_base import TradeFinderBase
            ctx = FinanceMappingContext()
            assert isinstance(ctx.account, AccountFinderBase)
            assert isinstance(ctx.trade, TradeFinderBase)
        finally:
            sys.path.remove(self.tmp)

    def test_find_all_has_uniform_signature(self):
        generate(self.mapping, self.tmp)
        trade_finder_path = os.path.join(self.tmp, "finance", "trade", "trade_finder.py")
        content = open(trade_finder_path).read()
        assert "def find_all(self," in content
        assert "business_date" in content
        assert "processing_valid_at" in content
        account_finder_path = os.path.join(self.tmp, "finance", "reference_data", "account_finder.py")
        content = open(account_finder_path).read()
        assert "def find_all(self," in content
        assert "business_date" in content
        assert "processing_valid_at" in content


class TestDualAssociationSameTarget:
    """Two associations from the same source class to the same target class
    (e.g. Contract.primary_owner and Contract.secondary_owner, both → Employee)
    must generate two distinct reverse navigation methods on EmployeeFinder.
    """

    def _build_mapping(self):
        from model.m3 import Integer
        from model.mapping import Mapping
        from model.relational import Database, Schema, Table, Column, ForeignKey
        from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

        pkg = Package("org")
        employee_cls = Class("Employee", [
            Property("Id", "id", Integer),
            Property("Name", "name", String),
        ], pkg)
        contract_cls = Class("Contract", [
            Property("Id", "id", Integer),
            Property("Primary Owner", "primaryOwner", employee_cls),
            Property("Secondary Owner", "secondaryOwner", employee_cls),
        ], pkg)
        Association("ContractPrimaryOwner", "Contract", Multiplicity.MANY, "primary_owner_contracts",
                    "Employee", Multiplicity.ONE, "primaryOwner", pkg)
        Association("ContractSecondaryOwner", "Contract", Multiplicity.MANY, "secondary_owner_contracts",
                    "Employee", Multiplicity.ONE, "secondaryOwner", pkg)

        repo = Database("org_db", "duckdb://org.db")
        schema = Schema("hr", repo)
        emp_id_col = Column("id", "INT", primary_key=True)
        emp_name_col = Column("name", "VARCHAR")
        emp_table = Table("employees", [emp_id_col, emp_name_col], schema)

        primary_fk_col = Column("primary_owner_id", "INT")
        secondary_fk_col = Column("secondary_owner_id", "INT")
        contract_id_col = Column("id", "INT", primary_key=True)
        contract_table = Table("contracts", [contract_id_col, primary_fk_col, secondary_fk_col], schema)

        emp_id_pm = RelationalPropertyMapping(employee_cls.property("id"), emp_id_col)
        emp_name_pm = RelationalPropertyMapping(employee_cls.property("name"), emp_name_col)
        emp_rcm = RelationalClassMapping(employee_cls, [emp_id_pm, emp_name_pm])

        contract_id_pm = RelationalPropertyMapping(contract_cls.property("id"), contract_id_col)
        primary_join = Join(primary_fk_col, emp_id_col)
        primary_pm = RelationalPropertyMapping(contract_cls.property("primaryOwner"), primary_join)
        secondary_join = Join(secondary_fk_col, emp_id_col)
        secondary_pm = RelationalPropertyMapping(contract_cls.property("secondaryOwner"), secondary_join)
        contract_rcm = RelationalClassMapping(contract_cls, [contract_id_pm, primary_pm, secondary_pm])

        return Mapping("OrgMapping", [emp_rcm, contract_rcm])

    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.mapping = self._build_mapping()

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
        for mod in list(sys.modules.keys()):
            if mod.startswith("org"):
                sys.modules.pop(mod, None)

    def test_employee_finder_has_both_reverse_methods(self):
        generate(self.mapping, self.tmp)
        content = open(os.path.join(self.tmp, "employee_finder.py")).read()
        assert "def primary_owner_contracts(" in content
        assert "def secondary_owner_contracts(" in content

    def test_reverse_methods_are_distinct_not_shadowed(self):
        generate(self.mapping, self.tmp)
        content = open(os.path.join(self.tmp, "employee_finder.py")).read()
        assert "primary_owner_id" in content
        assert "secondary_owner_id" in content

    def test_both_reverse_methods_callable_and_independent(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from employee_finder import EmployeeFinder
            emp = EmployeeFinder()
            primary = emp.primary_owner_contracts()
            secondary = emp.secondary_owner_contracts()
            assert primary is not None
            assert secondary is not None
            assert primary is not secondary
        finally:
            sys.path.remove(self.tmp)


class TestCamelCaseReverseAssocMethodName:
    """When an association's source_property is camelCase (e.g. 'relatedEntities'),
    the generated reverse navigation method on the target finder must be snake_case
    ('related_entities'), not the raw camelCase string.
    """

    def _build_mapping(self):
        from model.m3 import Integer
        from model.mapping import Mapping
        from model.relational import Database, Schema, Table, Column
        from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

        pkg = Package("org")
        entity_cls = Class("Entity", [
            Property("Id", "id", Integer),
        ], pkg)
        tag_cls = Class("Tag", [
            Property("Id", "id", Integer),
            Property("Related Entity", "relatedEntity", entity_cls),
        ], pkg)
        # source_property is camelCase: "relatedEntities"
        Association("TagEntity", "Tag", Multiplicity.MANY, "relatedEntities",
                    "Entity", Multiplicity.ONE, "relatedEntity", pkg)

        repo = Database("org_db", "duckdb://org.db")
        schema = Schema("data", repo)
        entity_id_col = Column("id", "INT", primary_key=True)
        entity_table = Table("entities", [entity_id_col], schema)

        tag_id_col = Column("id", "INT", primary_key=True)
        fk_col = Column("entity_id", "INT")
        tag_table = Table("tags", [tag_id_col, fk_col], schema)

        entity_id_pm = RelationalPropertyMapping(entity_cls.property("id"), entity_id_col)
        entity_rcm = RelationalClassMapping(entity_cls, [entity_id_pm])

        tag_id_pm = RelationalPropertyMapping(tag_cls.property("id"), tag_id_col)
        join = Join(fk_col, entity_id_col)
        tag_entity_pm = RelationalPropertyMapping(tag_cls.property("relatedEntity"), join)
        tag_rcm = RelationalClassMapping(tag_cls, [tag_id_pm, tag_entity_pm])

        return Mapping("OrgMapping", [entity_rcm, tag_rcm])

    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.mapping = self._build_mapping()

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
        for mod in list(sys.modules.keys()):
            if mod.startswith("org") or mod in ("entity_finder", "tag_finder",
                                                 "entity_finder_base", "tag_finder_base"):
                sys.modules.pop(mod, None)

    def test_reverse_method_is_snake_case(self):
        generate(self.mapping, self.tmp)
        content = open(os.path.join(self.tmp, "entity_finder.py")).read()
        assert "def related_entities(" in content

    def test_camel_case_method_not_generated(self):
        generate(self.mapping, self.tmp)
        content = open(os.path.join(self.tmp, "entity_finder.py")).read()
        assert "def relatedEntities(" not in content

    def test_reverse_method_is_callable(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from entity_finder import EntityFinder
            result = EntityFinder().related_entities()
            assert result is not None
        finally:
            sys.path.remove(self.tmp)
