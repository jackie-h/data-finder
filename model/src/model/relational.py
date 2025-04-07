from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping


class RelationalElement:
    def __init(self):
        pass


class Column(RelationalElement):
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type
        self.table = None


class Table:
    def __init__(self, name: str, columns: list[Column]):
        self.name = name
        self.columns = columns
        for col in columns:
            col.table = self


class Join(RelationalElement):
    def __init__(self, lhs: Column, rhs: Column):
        self.lhs = lhs
        self.rhs = rhs


class RelationalPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, target: RelationalElement):
        super().__init__(property, target)


class RelationalClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping]):
        super().__init__(clazz, property_mappings)



