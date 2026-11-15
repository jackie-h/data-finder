"""An embedded set mapping is a class mapped in a row, which is not relational; the relational one is also a relational operation element, so a relational property mapping can have it as its target."""
import pytest

from model.m3 import Class, Property, ONE_TO_ONE, PrimitiveType
from model.mapping import EmbeddedSetMapping, PropertyMapping
from model.relational import RelationalOperationElement
from model.relational_mapping import RelationalEmbeddedSetMapping, RelationalPropertyMapping

_PERSON = Class("Person", [Property("Name", "name", PrimitiveType("String"), ONE_TO_ONE)], None)


def test_an_embedded_set_mapping_is_not_a_relational_operation_element():
    embedded = EmbeddedSetMapping(_PERSON, [PropertyMapping(_PERSON.properties["name"], object(), "Firm_employees")], "Firm_employees")
    assert not isinstance(embedded, RelationalOperationElement) and embedded.id == "Firm_employees"


def test_the_relational_one_is_both():
    embedded = RelationalEmbeddedSetMapping(_PERSON, [RelationalPropertyMapping(_PERSON.properties["name"], object(), "Firm_employees")], "Firm_employees")  # pyright: ignore[reportArgumentType]
    assert isinstance(embedded, EmbeddedSetMapping) and isinstance(embedded, RelationalOperationElement)


def test_the_property_mappings_in_it_start_from_its_id():
    with pytest.raises(ValueError, match="embedded as Firm_employees has the source set id 'Person', not 'Firm_employees'"):
        RelationalEmbeddedSetMapping(_PERSON, [RelationalPropertyMapping(_PERSON.properties["name"], object(), "Person")], "Firm_employees")  # pyright: ignore[reportArgumentType]
