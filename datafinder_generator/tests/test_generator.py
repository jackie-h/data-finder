import os
import shutil
import sys
import tempfile

import pytest
from mapping_markdown.markdown_mapping import load
from model.m3 import Property, String, Class, Package, Association, Multiplicity
from datafinder_generator.generator import to_python_name, generate, _class_package, _ensure_package_dirs


def _prop(label: str, id: str = None) -> Property:
    return Property(label, id or label.lower().replace(' ', '_'), String)


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
        prop = Property("Valid From", "valid_from", String)
        assert to_python_name(prop) == "valid_from"


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

    def test_generated_finder_is_importable(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from finance.trade.trade_finder import TradeFinder
            assert TradeFinder is not None
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
            result = AccountFinder.trades()
            assert result is not None
        finally:
            sys.path.remove(self.tmp)


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
            Property("Primary Owner", "primary_owner", employee_cls),
            Property("Secondary Owner", "secondary_owner", employee_cls),
        ], pkg)
        Association("ContractPrimaryOwner", "Contract", Multiplicity.MANY, "primary_owner_contracts",
                    "Employee", Multiplicity.ONE, "primary_owner", pkg)
        Association("ContractSecondaryOwner", "Contract", Multiplicity.MANY, "secondary_owner_contracts",
                    "Employee", Multiplicity.ONE, "secondary_owner", pkg)

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
        primary_pm = RelationalPropertyMapping(contract_cls.property("primary_owner"), primary_join)
        secondary_join = Join(secondary_fk_col, emp_id_col)
        secondary_pm = RelationalPropertyMapping(contract_cls.property("secondary_owner"), secondary_join)
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
        # Both FK columns must appear — if one were shadowed, only one would be present
        assert "primary_owner_id" in content
        assert "secondary_owner_id" in content

    def test_both_reverse_methods_callable_and_independent(self):
        generate(self.mapping, self.tmp)
        sys.path.insert(0, self.tmp)
        try:
            from employee_finder import EmployeeFinder
            primary = EmployeeFinder.primary_owner_contracts()
            secondary = EmployeeFinder.secondary_owner_contracts()
            assert primary is not None
            assert secondary is not None
            assert primary is not secondary
        finally:
            sys.path.remove(self.tmp)
