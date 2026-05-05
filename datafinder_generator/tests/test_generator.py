import os
import shutil
import sys
import tempfile

import pytest
from mapping_markdown.markdown_mapping import load
from model.m3 import Property, String
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
        from model.relational import Repository, Schema, Table, Column
        self.tmp = tempfile.mkdtemp()
        repo = Repository("finance_db", "duckdb://test.db")
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
