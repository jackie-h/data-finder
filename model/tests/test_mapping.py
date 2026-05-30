import pytest
from model.m3 import Class, Property, String, Integer
from model.mapping import Mapping, ClassMapping


def _simple_class(name: str) -> Class:
    return Class(name, [Property("Id", "id", Integer)], None)


class TestMappingDuplicateClassValidation:

    def test_duplicate_class_raises(self):
        cls = _simple_class("Account")
        cm1 = ClassMapping(cls, [])
        cm2 = ClassMapping(cls, [])
        with pytest.raises(ValueError, match="Class 'Account' is mapped more than once"):
            Mapping("TestMapping", [cm1, cm2])

    def test_duplicate_class_by_name_raises(self):
        cls1 = _simple_class("Account")
        cls2 = _simple_class("Account")
        cm1 = ClassMapping(cls1, [])
        cm2 = ClassMapping(cls2, [])
        with pytest.raises(ValueError, match="Class 'Account' is mapped more than once"):
            Mapping("TestMapping", [cm1, cm2])

    def test_distinct_classes_allowed(self):
        cm1 = ClassMapping(_simple_class("Account"), [])
        cm2 = ClassMapping(_simple_class("Trade"), [])
        mapping = Mapping("TestMapping", [cm1, cm2])
        assert len(mapping.mappings) == 2

    def test_single_class_allowed(self):
        cm = ClassMapping(_simple_class("Account"), [])
        mapping = Mapping("TestMapping", [cm])
        assert len(mapping.mappings) == 1

    def test_error_message_includes_mapping_name(self):
        cls = _simple_class("Trade")
        cm1 = ClassMapping(cls, [])
        cm2 = ClassMapping(cls, [])
        with pytest.raises(ValueError, match="mapping 'FinanceMapping'"):
            Mapping("FinanceMapping", [cm1, cm2])
