from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping
from model.relational import RelationalOperationElement, Column


class Join(RelationalOperationElement):
    """The join a navigation property follows: from ``lhs`` to ``rhs``.

    A property mapped through several tables has the hops before the last one in ``via``, each a (from, to) pair of
    columns, in order; ``lhs`` and ``rhs`` are then the last hop, the one that reaches the target class's table."""

    def __init__(self, lhs: Column, rhs: Column, embedded: 'EmbeddedSetMapping | None' = None,
                 via: 'list[tuple[Column, Column]] | None' = None):
        super().__init__()
        self.lhs = lhs
        self.rhs = rhs
        self.embedded = embedded
        self.via: list[tuple[Column, Column]] = via or []
        # False while the join stands in for one whose columns a later part of the mapping gives
        self.resolved = True


class EmbeddedSetMapping(RelationalOperationElement):
    """A partial class mapping embedded inline in the owning (root) table.

    Represents a chained property (e.g. account.name) satisfied by a flat column on the
    root table, avoiding the join that would normally be required to reach it. Nested
    property_mappings entries may themselves target a Column (leaf) or a further nested
    EmbeddedSetMapping (e.g. account.branch.city) — all such columns live on the root table —
    or a Join, for a property of the embedded class that reaches another table (e.g.
    classification.system), whose first column is a column of the root table.
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