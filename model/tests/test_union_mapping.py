"""A class that is the union of sets: every set has an id of its own, and a navigation between classes that have sets says the set it starts from and the set it reaches."""
import pytest

from model.m3 import Class, Property, ONE_TO_ONE, ZERO_TO_MANY, PrimitiveType
from model.mapping import ClassMapping, Mapping, PropertyMapping, UnionMapping

_STRING = PrimitiveType("String")


def _classes():
    person = Class("Person", [Property("Name", "name", _STRING, ONE_TO_ONE)], None)
    firm = Class("Firm", [Property("Name", "name", _STRING, ONE_TO_ONE)], None)
    firm.properties["employees"] = Property("Employees", "employees", person, ZERO_TO_MANY)
    return firm, person


def _set(clazz, set_id):
    return ClassMapping(clazz, [], id=set_id)


def _navigation(firm, source=None, target=None):
    return PropertyMapping(firm.properties["employees"], object(), source_set_id=source, target_set_id=target)


def test_a_union_needs_its_sets_each_to_have_an_id_and_the_ids_to_differ():
    firm, _ = _classes()
    with pytest.raises(ValueError, match="every set of Firm needs an id"):
        UnionMapping(firm, [_set(firm, "big"), _set(firm, None)], [])
    with pytest.raises(ValueError, match="the sets of Firm have the id 'big' twice"):
        UnionMapping(firm, [_set(firm, "big"), _set(firm, "big")], [])
    with pytest.raises(ValueError, match="a union of sets has at least two sets"):
        UnionMapping(firm, [_set(firm, "big")], [])


def test_a_navigation_from_a_union_says_which_of_its_sets_it_starts_from():
    firm, person = _classes()
    union = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, target="a")])
    with pytest.raises(ValueError, match="the property employees of Firm starts from a union of sets: it needs a source set id, one of big, small"):
        Mapping("m", [union, ClassMapping(person, [])])
    union = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, source="nope")])
    with pytest.raises(ValueError, match="'nope' is not a set of Firm"):
        Mapping("m", [union, ClassMapping(person, [])])


def test_a_navigation_from_a_class_that_is_not_a_union_has_no_source_set():
    firm, person = _classes()
    with pytest.raises(ValueError, match="Firm is not a union of sets: the property employees has no source set id"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, source="big")]), ClassMapping(person, [])])


def test_a_navigation_to_a_union_says_which_of_its_sets_it_reaches_and_to_a_class_that_is_not_one_it_does_not():
    firm, person = _classes()
    people = UnionMapping(person, [_set(person, "staff"), _set(person, "contractors")], [])
    with pytest.raises(ValueError, match="the property employees of Firm reaches a union of sets: it needs a target set id, one of staff, contractors"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm)]), people])
    with pytest.raises(ValueError, match="'nope' is not a set of Person"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, target="nope")]), people])
    with pytest.raises(ValueError, match="Person is not a union of sets: the property employees has no target set id"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, target="staff")]), ClassMapping(person, [])])


def test_a_navigation_between_sets_that_names_real_sets_is_accepted():
    firm, person = _classes()
    firms = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, "big", "staff"), _navigation(firm, "small", "contractors")])
    people = UnionMapping(person, [_set(person, "staff"), _set(person, "contractors")], [])
    assert len(Mapping("m", [firms, people]).mappings) == 2


def test_a_property_that_is_not_a_navigation_has_no_set_ids():
    firm, person = _classes()
    name = PropertyMapping(firm.properties["name"], object())
    Mapping("m", [UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [name]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="the property name of Firm is not a navigation: it has no set ids"):
        Mapping("m", [ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), source_set_id="big")]), ClassMapping(person, [])])
