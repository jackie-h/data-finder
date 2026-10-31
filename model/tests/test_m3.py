import pytest

from model.m3 import (
    Package, Class, Association, Property, String, Integer, _name_to_camel_id, ONE_TO_ONE, ZERO_TO_MANY, ZERO_TO_ONE,
    ONE_TO_MANY, BoundedMultiplicity, UnboundedMultiplicity, parse_multiplicity,
)


class TestPackageChildren:

    def test_class_registered_in_package(self):
        pkg = Package("finance")
        cls = Class("Account", [Property("Id", "id", Integer, ONE_TO_ONE)], pkg)
        assert cls in pkg.children

    def test_multiple_classes_registered(self):
        pkg = Package("finance")
        cls1 = Class("Account", [], pkg)
        cls2 = Class("Trade", [], pkg)
        assert cls1 in pkg.children
        assert cls2 in pkg.children

    def test_association_registered_in_package(self):
        pkg = Package("finance")
        assoc = Association("TradeAccount", "Trade", ZERO_TO_MANY, "Trades", "trades", "Account", ONE_TO_ONE, "Account", "account", pkg)
        assert assoc in pkg.children

    def test_children_order_preserved(self):
        pkg = Package("finance")
        cls1 = Class("Account", [], pkg)
        cls2 = Class("Trade", [], pkg)
        assoc = Association("TradeAccount", "Trade", ZERO_TO_MANY, "Trades", "trades", "Account", ONE_TO_ONE, "Account", "account", pkg)
        assert pkg.children == [cls1, cls2, assoc]

    def test_empty_package_has_no_children(self):
        pkg = Package("empty")
        assert pkg.children == []

    def test_none_package_does_not_raise(self):
        cls = Class("Orphan", [], None)
        assert cls.package is None


class TestNameToCamelId:

    def test_single_lowercase_word(self):
        assert _name_to_camel_id("price") == "price"

    def test_single_titlecase_word(self):
        assert _name_to_camel_id("Price") == "price"

    def test_single_all_caps_word(self):
        assert _name_to_camel_id("USD") == "usd"

    def test_single_all_caps_two_letters(self):
        assert _name_to_camel_id("ID") == "id"

    def test_single_titlecase_two_letters(self):
        assert _name_to_camel_id("Id") == "id"

    def test_single_camel_case_preserved(self):
        assert _name_to_camel_id("validFrom") == "validFrom"

    def test_multi_word(self):
        assert _name_to_camel_id("Valid From") == "validFrom"

    def test_multi_word_first_all_caps(self):
        assert _name_to_camel_id("USD Rate") == "usdRate"

    def test_multi_word_lowercase_first(self):
        assert _name_to_camel_id("is settled") == "isSettled"

    def test_empty_string(self):
        assert _name_to_camel_id("") == ""


class TestAssociationPropertyNames:

    def test_source_property_name_is_required(self):
        with pytest.raises(ValueError, match="Source Property Name"):
            Association("TradeAccount", "Trade", ZERO_TO_MANY, "", "trades", "Account", ONE_TO_ONE, "Account", "account", None)

    def test_target_property_name_is_required(self):
        with pytest.raises(ValueError, match="Target Property Name"):
            Association("TradeAccount", "Trade", ZERO_TO_MANY, "Trades", "trades", "Account", ONE_TO_ONE, "", "account", None)


class TestPropertyMultiplicity:

    def test_multiplicity_is_required(self):
        with pytest.raises(TypeError):
            Property("Id", "id", Integer)  # type: ignore[call-arg]

    def test_multiplicity_must_be_a_multiplicity(self):
        with pytest.raises(ValueError, match="multiplicity must be a Multiplicity"):
            Property("Id", "id", Integer, None)  # type: ignore[arg-type]

    @pytest.mark.parametrize("multiplicity", [ONE_TO_ONE, ZERO_TO_ONE, ONE_TO_MANY, ZERO_TO_MANY,
                                              BoundedMultiplicity(0, 0), BoundedMultiplicity(2, 2),
                                              BoundedMultiplicity(2, 5), UnboundedMultiplicity(3)])
    def test_parse_round_trips_str(self, multiplicity):
        parsed = parse_multiplicity(str(multiplicity))
        assert str(parsed) == str(multiplicity)
        assert parsed.is_many() == multiplicity.is_many()

    @pytest.mark.parametrize("text", ["", "many", "1..", "..1", "a..b", "3..1", "-1", "1.5", "1 .. *",
                                      "1..**", "*..1", "*..*", "0..-1"])
    def test_parse_rejects_invalid_text(self, text):
        with pytest.raises(ValueError):
            parse_multiplicity(text)


    @pytest.mark.parametrize("text,expected", [("*", "*"), ("0", "0"), ("1", "1"), ("0..1", "0..1"), ("1..*", "1..*"),
                                               ("0..*", "*"), ("1..1", "1"), ("2", "2"), ("7", "7"), ("2..2", "2"), ("2..5", "2..5"),
                                               ("3..*", "3..*"), (" 1 ", "1")])
    def test_parse_accepts_exactly_the_documented_forms(self, text, expected):
        assert str(parse_multiplicity(text)) == expected
