"""A property whose type is a class is mapped by a NavigationPropertyMapping, which says the set it reaches as well as the one it starts from; a property that holds a
value is mapped by a PropertyMapping, which has no target."""
import pytest

from model.m3 import Class, Property, ONE_TO_ONE, ZERO_TO_MANY, PrimitiveType
from model.mapping import ClassMapping, Mapping, NavigationPropertyMapping, PropertyMapping, UnionMapping

_STRING = PrimitiveType("String")


def _classes():
    person = Class("Person", [Property("Name", "name", _STRING, ONE_TO_ONE)], None)
    firm = Class("Firm", [Property("Name", "name", _STRING, ONE_TO_ONE)], None)
    firm.properties["employees"] = Property("Employees", "employees", person, ZERO_TO_MANY)
    return firm, person


def test_a_property_mapping_has_a_source_and_no_target_and_a_navigation_has_both():
    firm, _ = _classes()
    value = PropertyMapping(firm.properties["name"], object(), "Firm")
    assert not hasattr(value, "target_set_id")
    with pytest.raises(TypeError):
        NavigationPropertyMapping(firm.properties["employees"], object(), "Firm")  # type: ignore[call-arg]
    navigation = NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "Person")
    assert isinstance(navigation, PropertyMapping) and (navigation.source_set_id, navigation.target_set_id) == ("Firm", "Person")


def test_a_navigation_reaches_the_class_mapping_of_the_class_it_is_typed_by():
    firm, person = _classes()
    Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "Person")]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="the property employees of Firm reaches 'Other', but the class mapping of Person is 'Person'"):
        Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "Other")]), ClassMapping(person, [])])


def test_a_navigation_to_a_class_that_is_not_mapped_reaches_the_class():
    firm, _ = _classes()
    with pytest.raises(ValueError, match="the property employees of Firm reaches 'Other', but Person has no class mapping, so it can only be 'Person'"):
        Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "Other")])])
    Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "Person")])])


def test_a_navigation_to_a_union_reaches_one_of_its_sets():
    firm, person = _classes()
    people = UnionMapping(person, [ClassMapping(person, [], id="staff"), ClassMapping(person, [], id="contractors")], [])
    Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", "staff")]), people])
    for wrong in ("Person", "nope"):
        with pytest.raises(ValueError, match=r"the property employees of Firm reaches '%s', which is not a set of Person \(staff, contractors\)" % wrong):
            Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], object(), "Firm", wrong)]), people])


def test_a_property_that_is_typed_by_a_class_is_mapped_as_a_navigation_and_one_that_is_not_is_not():
    firm, person = _classes()
    with pytest.raises(ValueError, match="the property employees of Firm is typed by a class: it is mapped by a NavigationPropertyMapping"):
        Mapping("m", [ClassMapping(firm, [PropertyMapping(firm.properties["employees"], object(), "Firm")]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="the property name of Firm is not typed by a class: it is not mapped by a NavigationPropertyMapping"):
        Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["name"], object(), "Firm", "Person")]), ClassMapping(person, [])])


def test_a_navigation_that_is_mapped_in_the_row_reaches_the_embedded_class_mapping():
    from model.mapping import EmbeddedClassMapping
    firm, person = _classes()
    embedded = EmbeddedClassMapping(person, [PropertyMapping(person.properties["name"], object(), "Firm_employees")], "Firm_employees")
    Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], embedded, "Firm", "Firm_employees")]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="reaches 'Person', but it is mapped by the embedded class mapping 'Firm_employees'"):
        Mapping("m", [ClassMapping(firm, [NavigationPropertyMapping(firm.properties["employees"], embedded, "Firm", "Person")]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="embedded as Firm_employees has the source set id 'Person', not 'Firm_employees'"):
        EmbeddedClassMapping(person, [PropertyMapping(person.properties["name"], object(), "Person")], "Firm_employees")
