from typing import Optional

from linkml_runtime.loaders import yaml_loader
from linkml_runtime.linkml_model import SchemaDefinition

from model.m3 import (
    Class, Package, Property,
    String, Integer, Float, Double, Decimal, Date, DateTime, Boolean,
    TaggedValue, Type
)

_RANGE_MAP: dict[str, Type] = {
    "string": String,
    "integer": Integer,
    "boolean": Boolean,
    "float": Float,
    "double": Double,
    "decimal": Decimal,
    "date": Date,
    "datetime": DateTime,
    "date_or_datetime": DateTime,
    "uri": String,
    "uriorcurie": String,
    "curie": String,
    "ncname": String,
    "objectidentifier": String,
    "nodeidentifier": String,
    "jsonpointer": String,
    "jsonpath": String,
    "sparqlpath": String,
    "time": String,
}


def _resolve_type(range_name: Optional[str]) -> Type:
    if range_name is None:
        return String
    return _RANGE_MAP.get(range_name.lower(), String)


def load_schema(path: str) -> Package:
    schema: SchemaDefinition = yaml_loader.load(path, target_class=SchemaDefinition)

    package = Package(schema.name or "default")

    for class_name, class_def in schema.classes.items():
        properties: list[Property] = []

        for slot_name, slot_def in (class_def.attributes or {}).items():
            tagged: list[TaggedValue] = []
            if slot_def.description:
                tagged.append(TaggedValue(TaggedValue.DOC, slot_def.description))
            prop_type = _resolve_type(slot_def.range)
            properties.append(Property(slot_name, prop_type, tagged or None))

        for slot_name in (class_def.slots or []):
            if slot_name in (class_def.attributes or {}):
                continue
            slot_def = schema.slots.get(slot_name)
            if slot_def is None:
                continue
            tagged = []
            if slot_def.description:
                tagged.append(TaggedValue(TaggedValue.DOC, slot_def.description))
            prop_type = _resolve_type(slot_def.range)
            properties.append(Property(slot_name, prop_type, tagged or None))

        tagged_values: list[TaggedValue] = []
        if class_def.description:
            tagged_values.append(TaggedValue(TaggedValue.DOC, class_def.description))

        Class(class_name, properties, package, tagged_values or None)

    return package
