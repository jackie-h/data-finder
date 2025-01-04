from typing import Any

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, source: Property, target: Any):
        self.source = target

class ClassMapping:
    def __init__(self, clazz: Class, propertyMappings: list[PropertyMapping]):
        self.clazz = clazz
        self.propertyMappings = propertyMappings