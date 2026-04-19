import os
import tempfile

from model_markdown.markdown_model import load, save, loads, to_markdown, TAG_KEY
from model.m3 import String, Integer, Float, TaggedValue, Class, Association

FIXTURE = os.path.join(os.path.dirname(__file__), "finance.md")


class TestMarkdownLoad:

    def setup_method(self):
        self.packages, self.classes, self.associations = load(FIXTURE)
        self.by_name = {c.name: c for c in self.classes}

    def test_package_name(self):
        assert len(self.packages) == 1
        assert self.packages[0].name == "finance"

    def test_classes_loaded(self):
        assert set(self.by_name.keys()) == {"Account", "Instrument", "Trade"}

    def test_account_properties(self):
        account = self.by_name["Account"]
        assert account.property("id").type == Integer
        assert account.property("name").type == String

    def test_key_indicator(self):
        account = self.by_name["Account"]
        assert TAG_KEY in account.property("id").tagged_values
        assert TAG_KEY not in account.property("name").tagged_values

    def test_class_description(self):
        account = self.by_name["Account"]
        assert "Trading account" in account.tagged_values[TaggedValue.DOC].value

    def test_property_description(self):
        trade = self.by_name["Trade"]
        assert "symbol" in trade.property("symbol").tagged_values[TaggedValue.DOC].value.lower()

    def test_cross_class_reference(self):
        trade = self.by_name["Trade"]
        account_class = self.by_name["Account"]
        assert trade.property("account").type is account_class

    def test_association_loaded(self):
        assert len(self.associations) == 1
        assoc = self.associations[0]
        assert assoc.name == "TradeAccount"
        assert assoc.source == "Trade"
        assert assoc.target == "Account"

    def test_association_description(self):
        assoc = self.associations[0]
        assert "Links a trade" in assoc.tagged_values[TaggedValue.DOC].value

    def test_class_package(self):
        for cls in self.classes:
            assert cls.package is self.packages[0]


class TestMarkdownSave:

    def setup_method(self):
        self.packages, self.classes, self.associations = load(FIXTURE)

    def test_roundtrip(self):
        content = to_markdown("Finance Model", self.classes, self.associations)
        packages2, classes2, associations2 = loads(content)

        by_name = {c.name: c for c in classes2}
        assert set(by_name.keys()) == {"Account", "Instrument", "Trade"}
        assert by_name["Account"].property("id").type == Integer
        assert TAG_KEY in by_name["Account"].property("id").tagged_values
        assert len(associations2) == 1
        assert associations2[0].name == "TradeAccount"

    def test_save_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w") as f:
            path = f.name
        try:
            save(path, "Finance Model", self.classes, self.associations)
            packages2, classes2, _ = load(path)
            assert len(classes2) == 3
        finally:
            os.unlink(path)

    def test_generated_markdown_has_sub_domain(self):
        content = to_markdown("Finance Model", self.classes, self.associations)
        assert "## Sub-Domain: finance" in content

    def test_generated_markdown_has_association(self):
        content = to_markdown("Finance Model", self.classes, self.associations)
        assert "### Association: TradeAccount" in content
