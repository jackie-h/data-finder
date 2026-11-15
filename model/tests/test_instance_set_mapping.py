"""An instance set mapping is a class mapped by the mappings of its properties, with an id: the common part of a class mapping and an embedded one. It is abstract."""
import pytest

from model.m3 import Class, Property, ONE_TO_ONE, PrimitiveType
from model.mapping import ClassMapping, EmbeddedSetMapping, InstanceSetMapping, PropertyMapping, UnionMapping

_FIRM = Class("Firm", [Property("Name", "name", PrimitiveType("String"), ONE_TO_ONE)], None)


def test_an_instance_set_mapping_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        InstanceSetMapping(_FIRM, [], "Firm")  # pyright: ignore[reportAbstractUsage]


def test_a_class_mapping_and_an_embedded_set_mapping_are_instance_set_mappings_and_neither_is_the_other():
    firm = ClassMapping(_FIRM, [])
    embedded = EmbeddedSetMapping(_FIRM, [], "Trade_firm")
    assert isinstance(firm, InstanceSetMapping) and isinstance(embedded, InstanceSetMapping)
    assert not isinstance(embedded, ClassMapping) and not isinstance(firm, EmbeddedSetMapping)
    assert isinstance(UnionMapping(_FIRM, [ClassMapping(_FIRM, [], id="a"), ClassMapping(_FIRM, [], id="b")], []), ClassMapping)


def test_they_share_the_class_the_property_mappings_and_the_id():
    name = PropertyMapping(_FIRM.properties["name"], object(), "Firm")
    firm = ClassMapping(_FIRM, [name])
    embedded = EmbeddedSetMapping(_FIRM, [PropertyMapping(_FIRM.properties["name"], object(), "Trade_firm")], "Trade_firm")
    assert (firm.clazz, list(firm.property_mappings), firm.id) == (_FIRM, [name], "Firm")  # the id is the class unless it is given another
    assert (embedded.clazz, embedded.id) == (_FIRM, "Trade_firm") and len(embedded.property_mappings) == 1


def test_only_a_class_mapping_has_milestoning():
    assert ClassMapping(_FIRM, []).milestone_mapping is None
    assert not hasattr(EmbeddedSetMapping(_FIRM, [], "Trade_firm"), "milestone_mapping")
