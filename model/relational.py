from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping


class Column:
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type


class Table:
    def __init__(self, name: str, columns: list[Column]):
        self.name = name
        self.columns = columns


class RelationalPropertyMapping(PropertyMapping):
    def __init__(self, source: Property, target: Column):
        super().__init__(source, target)


class RelationalClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping]):
        super().__init__(clazz, property_mappings)
