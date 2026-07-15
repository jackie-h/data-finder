from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping
from model.relational import RelationalOperationElement, Column


class Join(RelationalOperationElement):
    def __init__(self, lhs: Column, rhs: Column, embedded: 'EmbeddedSetMapping | None' = None):
        super().__init__()
        self.lhs = lhs
        self.rhs = rhs
        self.embedded = embedded


class EmbeddedSetMapping(RelationalOperationElement):
    """A partial class mapping embedded inline in the owning (root) table.

    Represents a chained property (e.g. account.name) satisfied by a flat column on the
    root table, avoiding the join that would normally be required to reach it. Nested
    property_mappings entries may themselves target a Column (leaf) or a further nested
    EmbeddedSetMapping (e.g. account.branch.city) — all such columns live on the root table.
    """
    def __init__(self, clazz: Class, property_mappings: list['RelationalPropertyMapping']):
        super().__init__()
        self.clazz = clazz
        self.property_mappings = property_mappings


class RelationalPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, target: RelationalOperationElement):
        super().__init__(property, target)


class RelationalClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping)