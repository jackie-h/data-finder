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


def _navigation(firm, source, target=None):
    return PropertyMapping(firm.properties["employees"], object(), source, target)


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
    union = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, "Firm")])
    with pytest.raises(ValueError, match=r"'Firm' is not a set of Firm \(big, small\): the property employees of Firm starts from one of them"):
        Mapping("m", [union, ClassMapping(person, [])])
    union = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, "nope")])
    with pytest.raises(ValueError, match="'nope' is not a set of Firm"):
        Mapping("m", [union, ClassMapping(person, [])])


def test_a_navigation_from_a_class_that_is_not_a_union_starts_from_its_class_mapping():
    firm, person = _classes()
    Mapping("m", [ClassMapping(firm, [_navigation(firm, "Firm")]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="the property employees of Firm has the source set id 'big', but the class mapping it belongs to is 'Firm'"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, "big")]), ClassMapping(person, [])])


def test_a_navigation_to_a_union_says_which_of_its_sets_it_reaches_and_to_a_class_that_is_not_one_it_does_not():
    firm, person = _classes()
    people = UnionMapping(person, [_set(person, "staff"), _set(person, "contractors")], [])
    with pytest.raises(ValueError, match="the property employees of Firm reaches a union of sets: it needs a target set id, one of staff, contractors"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, "Firm")]), people])
    with pytest.raises(ValueError, match="'nope' is not a set of Person"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, "Firm", "nope")]), people])
    with pytest.raises(ValueError, match="Person is not a union of sets: the property employees has no target set id"):
        Mapping("m", [ClassMapping(firm, [_navigation(firm, "Firm", "staff")]), ClassMapping(person, [])])


def test_a_navigation_between_sets_that_names_real_sets_is_accepted():
    firm, person = _classes()
    firms = UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [_navigation(firm, "big", "staff"), _navigation(firm, "small", "contractors")])
    people = UnionMapping(person, [_set(person, "staff"), _set(person, "contractors")], [])
    assert len(Mapping("m", [firms, people]).mappings) == 2


def test_a_property_that_is_not_a_navigation_has_no_target_set_id():
    firm, person = _classes()
    name = PropertyMapping(firm.properties["name"], object(), "Firm")
    Mapping("m", [UnionMapping(firm, [_set(firm, "big"), _set(firm, "small")], [name]), ClassMapping(person, [])])
    with pytest.raises(ValueError, match="the property name of Firm is not a navigation: it has no target set id"):
        Mapping("m", [ClassMapping(firm, [PropertyMapping(firm.properties["name"], object(), "Firm", "big")]), ClassMapping(person, [])])
