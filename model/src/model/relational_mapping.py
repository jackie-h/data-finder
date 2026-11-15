from model.m3 import Property, Class
from model.mapping import ClassMapping, EmbeddedSetMapping, PropertyMapping, MilestonePropertyMapping, UnionMapping
from model.relational import Operation, RelationalOperationElement, Column, Table, TableAlias


class Join(RelationalOperationElement):
    """The join a navigation property follows: from ``lhs`` to ``rhs``.

    A property mapped through several tables has the hops before the last one in ``via``, each a (from, to) pair of
    columns, in order; ``lhs`` and ``rhs`` are then the last hop, the one that reaches the target class's table.

    A hop can also have a condition besides its key columns, such as ``firmTable.ID = personTable.FIRMID and
    firmTable.FLAG is null``. ``conditions`` has one for each hop, the ``via`` ones and then the last, or ``None`` for a hop
    with only its keys. It is a condition over the columns of the two tables of that hop, so it reads the same from either
    end of the join; ``condition_texts`` is how it was written."""

    def __init__(self, lhs: Column, rhs: Column, embedded: 'RelationalEmbeddedSetMapping | None' = None,
                 via: 'list[tuple[Column, Column]] | None' = None,
                 conditions: 'list[Operation | None] | None' = None, condition_texts: 'list[str | None] | None' = None):
        super().__init__()
        self.lhs = lhs
        self.rhs = rhs
        self.embedded = embedded
        self.via: list[tuple[Column, Column]] = via or []
        hops = len(self.via) + 1
        self.conditions: list[Operation | None] = list(conditions or [])[:hops] + [None] * (hops - len(conditions or []))
        self.condition_texts: list[str | None] = list(condition_texts or [])[:hops] + [None] * (hops - len(condition_texts or []))
        # False while the join stands in for one whose columns a later part of the mapping gives
        self.resolved = True

    @property
    def condition(self) -> Operation | None:
        """The condition of the last hop, the one that reaches the target class's table."""
        return self.conditions[-1]

    @property
    def via_conditions(self) -> list[Operation | None]:
        """The conditions of the hops before the last."""
        return self.conditions[:-1]


class RelationalPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, target: RelationalOperationElement, source_set_id: str, target_set_id: str | None = None):
        super().__init__(property, target, source_set_id, target_set_id)


class RelationalClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None,
                 id: str | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping, id)

    @property
    def main_relation(self) -> Table | TableAlias | None:
        """What the class is mapped to: the table its properties are columns of, or the query that it is mapped to, whose columns they are."""
        return next((pm.target.table for pm in self.property_mappings if isinstance(pm.target, Column) and pm.target.table is not None), None)

    @property
    def main_table_alias(self) -> TableAlias | None:
        """What the class is mapped to, under a name: the query's own alias, or an alias of the table named
        as the table is. Its ``relation`` is the table or the query."""
        relation = self.main_relation
        if relation is None or isinstance(relation, TableAlias):
            return relation
        return TableAlias(relation, relation.name)


class RelationalUnionMapping(UnionMapping[RelationalClassMapping], RelationalClassMapping):
    """A class that is the union of relational sets: its sets, each a class mapping to a query, and the union of their queries, a query of its own that its
    property mappings are on the columns of."""

    def __init__(self, clazz: Class, sets: list[RelationalClassMapping], property_mappings: list[RelationalPropertyMapping],
                 milestone_mapping: MilestonePropertyMapping | None = None, id: str | None = None):
        super().__init__(clazz, sets, property_mappings, milestone_mapping, id)


class RelationalEmbeddedSetMapping(EmbeddedSetMapping, RelationalOperationElement):
    """A class embedded in the row of a table: its properties are columns of that row, further embedded mappings, or joins to other tables, as the property mappings
    of a relational class mapping are. A relational property mapping can have it as its target."""
    property_mappings: list[RelationalPropertyMapping]  # type: ignore[assignment]

    def __init__(self, clazz: Class, property_mappings: list[RelationalPropertyMapping], id: str):
        EmbeddedSetMapping.__init__(self, clazz, property_mappings, id)
        RelationalOperationElement.__init__(self)
