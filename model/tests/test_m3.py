from model.m3 import Package, Class, Association, Property, String, Integer, _name_to_camel_id, ONE_TO_ONE, ZERO_TO_MANY


class TestPackageChildren:

    def test_class_registered_in_package(self):
        pkg = Package("finance")
        cls = Class("Account", [Property("Id", "id", Integer)], pkg)
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
