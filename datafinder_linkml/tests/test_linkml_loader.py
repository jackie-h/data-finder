import os

from datafinder_linkml.linkml_loader import load_schema
from model.m3 import Class, String, Integer, Float, DateTime, Boolean, TaggedValue

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "finance.yaml")


class TestLinkMLLoader:

    def setup_method(self):
        self.package = load_schema(SCHEMA_PATH)
        self.classes = [c for c in self.package.children if isinstance(c, Class)]
        self.by_name = {c.name: c for c in self.classes}

    def test_package_name(self):
        assert self.package.name == "finance"

    def test_classes_loaded(self):
        assert set(self.by_name.keys()) == {"Account", "Instrument", "Trade"}

    def test_account_properties(self):
        account = self.by_name["Account"]
        assert account.property("id").type == Integer
        assert account.property("name").type == String

    def test_instrument_properties(self):
        instrument = self.by_name["Instrument"]
        assert instrument.property("symbol").type == String
        assert instrument.property("price").type == Float

    def test_trade_properties(self):
        trade = self.by_name["Trade"]
        assert trade.property("symbol").type == String
        assert trade.property("price").type == Float
        assert trade.property("account_id").type == Integer
        assert trade.property("valid_from").type == DateTime
        assert trade.property("valid_to").type == DateTime
        assert trade.property("is_settled").type == Boolean

    def test_class_description(self):
        account = self.by_name["Account"]
        assert TaggedValue.DOC in account.tagged_values
        assert "Trading account" in account.tagged_values[TaggedValue.DOC].value

    def test_property_description(self):
        trade = self.by_name["Trade"]
        symbol = trade.property("symbol")
        assert TaggedValue.DOC in symbol.tagged_values
        assert "symbol" in symbol.tagged_values[TaggedValue.DOC].value.lower()

    def test_class_package(self):
        for cls in self.classes:
            assert cls.package is self.package
