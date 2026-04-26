from model.m3 import Package, Class, Association, Property, String, Integer


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
        assoc = Association("TradeAccount", "Trade", "Account", pkg)
        assert assoc in pkg.children

    def test_children_order_preserved(self):
        pkg = Package("finance")
        cls1 = Class("Account", [], pkg)
        cls2 = Class("Trade", [], pkg)
        assoc = Association("TradeAccount", "Trade", "Account", pkg)
        assert pkg.children == [cls1, cls2, assoc]

    def test_empty_package_has_no_children(self):
        pkg = Package("empty")
        assert pkg.children == []

    def test_none_package_does_not_raise(self):
        cls = Class("Orphan", [], None)
        assert cls.package is None
