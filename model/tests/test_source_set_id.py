"""Every class mapping has an id, and every property mapping the id of the class mapping it belongs to: its source set id."""
import pytest

from model.m3 import Class, Property, ONE_TO_ONE, PrimitiveType
from model.mapping import ClassMapping, Mapping, PropertyMapping, UnionMapping

_STRING = PrimitiveType("String")


def _firm():
    return Class("Firm", [Property("Name", "name", _STRING, ONE_TO_ONE)], None)


def test_a_property_mapping_needs_its_source_set_id():
    firm = _firm()
    with pytest.raises(TypeError):
        PropertyMapping(firm.properties["name"], object())  # type: ignore[call-arg]
    assert PropertyMapping(firm.properties["name"], object(), "Firm").source_set_id == "Firm"


def test_a_class_mapping_has_an_id_that_is_its_class_unless_it_is_given_one():
    firm = _firm()
    assert ClassMapping(firm, []).id == "Firm"
    assert ClassMapping(firm, [], id="big").id == "big"
    assert UnionMapping(firm, [ClassMapping(firm, [], id="a"), ClassMapping(firm, [], id="b")], []).id == "Firm"


def test_a_property_mapping_is_the_source_set_of_the_class_mapping_it_belongs_to():
    firm = _firm()
    name = PropertyMapping(firm.properties["name"], object(), "Firm")
    assert Mapping("m", [ClassMapping(firm, [name])]).mappings[0].property_mappings == [name]
    with pytest.raises(ValueError, match="the property name of Firm has the source set id 'Other', but the class mapping it belongs to is 'Firm'"):
        Mapping("m", [ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), "Other")])])


def test_the_properties_of_a_set_are_its_own_and_those_of_a_union_are_the_unions():
    firm = _firm()
    a = ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), "a")], id="a")
    b = ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), "b")], id="b")
    Mapping("m", [UnionMapping(firm, [a, b], [PropertyMapping(firm.properties["name"], object(), "Firm")])])
    wrong = ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), "b")], id="a")
    with pytest.raises(ValueError, match="the property name of Firm has the source set id 'b', but the class mapping it belongs to is 'a'"):
        Mapping("m", [UnionMapping(firm, [wrong, b], [PropertyMapping(firm.properties["name"], object(), "Firm")])])
    with pytest.raises(ValueError, match="the property name of Firm has the source set id 'a', but the class mapping it belongs to is 'Firm'"):
        Mapping("m", [UnionMapping(firm, [a, b], [PropertyMapping(firm.properties["name"], object(), "a")])])


def test_the_ids_of_the_class_mappings_and_the_sets_are_all_different():
    firm, person = _firm(), Class("Person", [], None)
    with pytest.raises(ValueError, match="the id 'big' is the id of more than one class mapping"):
        Mapping("m", [ClassMapping(firm, [], id="big"), ClassMapping(person, [], id="big")])
    with pytest.raises(ValueError, match="the id 'Person' is the id of more than one class mapping"):
        Mapping("m", [UnionMapping(firm, [ClassMapping(firm, [], id="Person"), ClassMapping(firm, [], id="b")], []), ClassMapping(person, [])])
