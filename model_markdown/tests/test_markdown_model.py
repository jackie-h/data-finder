import logging
import os
import tempfile

from model_markdown.markdown_model import load, save, loads, to_markdown
from model.m3 import String, Integer, Double, TaggedValue, Class, Association, Package

FIXTURE = os.path.join(os.path.dirname(__file__), "finance.md")


class TestMarkdownLoad:

    def setup_method(self):
        self.packages = load(FIXTURE)
        self.pkg = self.packages[0]
        self.classes = [c for c in self.pkg.children if isinstance(c, Class)]
        self.associations = [a for a in self.pkg.children if isinstance(a, Association)]
        self.by_name = {c.name: c for c in self.classes}

    def test_package_name(self):
        assert len(self.packages) == 1
        assert self.pkg.name == "finance"

    def test_classes_in_package_children(self):
        assert set(self.by_name.keys()) == {"Account", "Instrument", "Trade"}

    def test_account_properties(self):
        account = self.by_name["Account"]
        assert account.property("id").type == Integer
        assert account.property("name").type == String

    def test_key_indicator(self):
        account = self.by_name["Account"]
        assert TaggedValue.KEY in account.property("id").tagged_values
        assert TaggedValue.KEY not in account.property("name").tagged_values

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

    def test_association_in_package_children(self):
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
            assert cls.package is self.pkg


class TestMarkdownUnexpectedColumns:

    def test_extra_property_column_ignored(self, caplog):
        content = """
## Sub-Domain: finance

### Class: Account

| Name    | Description |
|---------|-------------|
| Account |             |

| Property | Type    | Key | Description | Notes         |
|----------|---------|-----|-------------|---------------|
| id       | Integer | Y   |             | internal only |
| name     | String  |     |             |               |
"""
        with caplog.at_level(logging.WARNING, logger="model_markdown.markdown_model"):
            packages = loads(content)
        account = [c for c in packages[0].children if isinstance(c, Class)][0]
        assert account.property("id").type == Integer
        assert account.property("name").type == String
        assert TaggedValue.KEY in account.property("id").tagged_values
        assert any("Notes" in m for m in caplog.messages)

    def test_extra_class_header_column_ignored(self, caplog):
        content = """
## Sub-Domain: finance

### Class: Account

| Name    | Description     | Owner   |
|---------|-----------------|---------|
| Account | Trading account | finance |

| Property | Type    | Key | Description |
|----------|---------|-----|-------------|
| id       | Integer | Y   |             |
"""
        with caplog.at_level(logging.WARNING, logger="model_markdown.markdown_model"):
            packages = loads(content)
        account = [c for c in packages[0].children if isinstance(c, Class)][0]
        assert account.name == "Account"
        assert "Trading account" in account.tagged_values[TaggedValue.DOC].value
        assert any("Owner" in m for m in caplog.messages)

    def test_extra_association_column_ignored(self, caplog):
        content = """
## Sub-Domain: finance

### Association: TradeAccount

| Name         | Source | Target  | Description | Cardinality |
|--------------|--------|---------|-------------|-------------|
| TradeAccount | Trade  | Account | A link      | many-to-one |
"""
        with caplog.at_level(logging.WARNING, logger="model_markdown.markdown_model"):
            packages = loads(content)
        assoc = [a for a in packages[0].children if isinstance(a, Association)][0]
        assert assoc.source == "Trade"
        assert assoc.target == "Account"
        assert any("Cardinality" in m for m in caplog.messages)


class TestMarkdownSave:

    def setup_method(self):
        self.packages = load(FIXTURE)

    def test_roundtrip(self):
        content = to_markdown("Finance Model", self.packages)
        packages2 = loads(content)

        classes2 = [c for c in packages2[0].children if isinstance(c, Class)]
        assocs2 = [a for a in packages2[0].children if isinstance(a, Association)]
        by_name = {c.name: c for c in classes2}
        assert set(by_name.keys()) == {"Account", "Instrument", "Trade"}
        assert by_name["Account"].property("id").type == Integer
        assert TaggedValue.KEY in by_name["Account"].property("id").tagged_values
        assert len(assocs2) == 1
        assert assocs2[0].name == "TradeAccount"

    def test_save_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w") as f:
            path = f.name
        try:
            save(path, "Finance Model", self.packages)
            packages2 = load(path)
            classes2 = [c for c in packages2[0].children if isinstance(c, Class)]
            assert len(classes2) == 3
        finally:
            os.unlink(path)

    def test_generated_markdown_has_sub_domain(self):
        content = to_markdown("Finance Model", self.packages)
        assert "## Sub-Domain: finance" in content

    def test_generated_markdown_has_association(self):
        content = to_markdown("Finance Model", self.packages)
        assert "### Association: TradeAccount" in content
