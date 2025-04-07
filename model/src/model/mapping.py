from typing import Any

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any):
        self.property = property
        self.target = target

class ClassMapping:
    def __init__(self, clazz: Class, property_mappings: list[PropertyMapping]):
        self.clazz = clazz
        self.property_mappings = property_mappings

class Mapping:
    def __init__(self, name: str, mappings: list[ClassMapping]):
        self.name = name
        self.mappings = mappings