from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping
from model.relational import RelationalOperationElement


class RelationalPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, target: RelationalOperationElement):
        super().__init__(property, target)


class RelationalClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping], milestone_mapping: MilestonePropertyMapping = None):
        super().__init__(clazz, property_mappings, milestone_mapping)