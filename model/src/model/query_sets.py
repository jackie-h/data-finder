"""A class mapped to several queries: each is a set, with a name of its own, and the class mapping that is read is their union.

A union of queries has to be sure of two things that a single query does not. Rows of different sets can look the same (a key of 1 can be in
both), so each row says which set it came from, and a set's key must be one that no two rows of the set share. And what a navigation joins
across sets has to be said, set by set. This module checks the first: that the sets agree, and that each set's key is a key; the union it
makes gives each row the name of its set in the column ``_set``.
"""
from __future__ import annotations

import copy
from collections.abc import Sequence

from model.relational import (
    Alias, Column, ComparisonOperation, ComparisonOperator, JoinKind, LogicalOperation, LogicalOperator, Operation, QuerySpecification,
    StringConstantOperation, Table, TableAlias, UnionOperation,
)
from model.m3 import Class
from model.mapping import ClassMapping, EmbeddedSetMapping, NavigationPropertyMapping, PropertyMapping
from model.relational_mapping import Join, RelationalClassMapping, RelationalNavigationPropertyMapping, RelationalPropertyMapping, RelationalUnionMapping

SET_COLUMN = "_set"


def check_sets(class_name: str, sets: Sequence[RelationalClassMapping]) -> None:
    """Refuse the sets of a class that cannot be put together as a union with a key that tells their rows apart."""
    first = sets[0]
    for other in sets:
        alias = _alias(other)
        if SET_COLUMN in [c.name for c in alias.columns if isinstance(c, Column)]:
            raise ValueError(f"the set '{other.id}' of {class_name} gives a column named {SET_COLUMN}: {SET_COLUMN} is the column that says "
                             f"which set a row came from")
    for other in sets[1:]:
        _agree(class_name, first, other)
    for each in sets:
        _check_key(class_name, each)


def _alias(rcm: RelationalClassMapping) -> TableAlias:
    alias = rcm.main_relation
    assert isinstance(alias, TableAlias)
    return alias


def _columns(rcm: RelationalClassMapping) -> list[Column]:
    return [c for c in _alias(rcm).columns if isinstance(c, Column)]


def _agree(class_name: str, first: RelationalClassMapping, other: RelationalClassMapping) -> None:
    pair = f"the sets '{first.id}' and '{other.id}' of {class_name}"
    a, b = _columns(first), _columns(other)
    if [c.name for c in a] != [c.name for c in b]:
        raise ValueError(f"{pair} give different columns: {[c.name for c in a]} and {[c.name for c in b]}")
    if [c.type.upper() for c in a] != [c.type.upper() for c in b]:
        raise ValueError(f"{pair} give different types: {[c.type for c in a]} and {[c.type for c in b]}")

    scheme_a, scheme_b = _alias(first).milestoning_scheme, _alias(other).milestoning_scheme
    if (scheme_a.name if scheme_a else None) != (scheme_b.name if scheme_b else None):
        raise ValueError(f"{pair} have different milestoning: {scheme_a.name if scheme_a else 'none'} and {scheme_b.name if scheme_b else 'none'}")

    def properties(rcm):
        return {pm.target.name: pm.property.id for pm in rcm.property_mappings if isinstance(pm.target, Column)}
    if properties(first) != properties(other):
        raise ValueError(f"{pair} map the columns to different properties: {properties(first)} and {properties(other)}")


def _check_key(class_name: str, rcm: RelationalClassMapping) -> None:
    where = f"the set '{rcm.id}' of {class_name}"
    alias = _alias(rcm)
    key = [c.name for c in _columns(rcm) if c.primary_key]
    if not key:
        raise ValueError(f"{where} has no key column: mark the columns that tell one of its rows from another with Key PK")
    spec = alias.relation
    assert isinstance(spec, QuerySpecification)
    if spec.distinct:
        every = [c.name for c in _columns(rcm)]
        if sorted(key) != sorted(every):
            raise ValueError(f"{where} is DISTINCT, so every column of it is its key: {every}, not {key}")
        return
    if spec.group_by:
        grouped = sorted(_name_of(g) for g in spec.group_by)
        if grouped != sorted(key):
            raise ValueError(f"{where} is grouped by {grouped} but its key is {sorted(key)}: a group is a row, so what it is grouped by is its key")
        return
    _root_key_is_selected(where, spec, key)
    for join in spec.joins:
        _join_is_to_a_key(where, join)


def _name_of(element) -> str:
    return element.name if isinstance(element, (Column, Alias)) else str(element)


def _key_names(relation) -> list[str]:
    return [c.name for c in relation.columns if isinstance(c, Column) and c.primary_key]


def _root_key_is_selected(where: str, spec: QuerySpecification, key: list[str]) -> None:
    root = spec.table
    names = _key_names(root)
    label = getattr(root, "name", "the table")
    if not names:
        raise ValueError(f"{where} reads {label}, which has no primary key, so nothing says that its rows are different")
    selected = {item.name for item in spec.columns
                if isinstance(item, Alias) and isinstance(item.element, Column) and item.element.owner is root and item.element.name in names}
    if sorted(selected) != sorted(names) or any(item_name not in key for item_name in _output_names_of(spec, names)):
        raise ValueError(f"{where} does not select the key of {label} ({', '.join(names)}): two of its rows could then be the same")


def _output_names_of(spec: QuerySpecification, source_names: list[str]) -> list[str]:
    return [item.name for item in spec.columns
            if isinstance(item, Alias) and isinstance(item.element, Column) and item.element.name in source_names]


def _equated(operation, target) -> set[str]:
    """The columns of ``target`` that a join's condition sets equal to something, when it is all ANDed equalities."""
    if isinstance(operation, LogicalOperation) and operation.operator == LogicalOperator.AND:
        return _equated(operation.left, target) | _equated(operation.right, target)
    if isinstance(operation, ComparisonOperation) and operation.operator == ComparisonOperator.EQUAL:
        return {side.name for side in (operation.left, operation.right) if isinstance(side, Column) and side.owner is target}
    return set()


def _join_is_to_a_key(where: str, join) -> None:
    target = join.target
    label = getattr(target.relation, "name", None) if isinstance(target, TableAlias) and isinstance(target.relation, Table) else getattr(target, "name", "a table")
    if join.kind in (JoinKind.RIGHT, JoinKind.FULL):
        raise ValueError(f"{where} joins {label} with a {join.kind.name} join, which can add rows that have no row of the set's table")
    names = _key_names(target)
    if not names or not set(names) <= _equated(join.operation, target):
        shown = f" ({', '.join(names)})" if names else " (it has none)"
        raise ValueError(f"{where} joins {label} on columns that are not its key{shown}: a row of the set can match several of its rows, "
                         f"and then repeat")


def union_alias(name: str, sets: Sequence[tuple[TableAlias, str]], where: str) -> TableAlias:
    """The queries of the sets put together, each with its id, as one query under ``name``: ``UNION ALL`` (a row of one set is never dropped because a
    row of another looks the same), with a column ``_set`` that says which set each row is of. The columns are those of the first set."""
    branches = []
    for each, set_id in sets:
        spec = copy.copy(each.relation)
        if not isinstance(spec, QuerySpecification):
            raise ValueError(f"{where}: the set '{set_id}' is not a single SELECT")
        spec.columns = [*spec.columns, Alias(StringConstantOperation(set_id), SET_COLUMN)]
        branches.append(spec)
    union = branches[0]
    for branch in branches[1:]:
        union = UnionOperation(union, branch, all=True)
    first = sets[0][0]
    columns = [Column(c.name, c.type, primary_key=c.primary_key, nullable=c.nullable) for c in first.columns if isinstance(c, Column)] \
        + [Column(SET_COLUMN, "VARCHAR")]
    alias = TableAlias(union, name, columns)
    alias.reads = [r for each, _ in sets for r in each.reads]
    alias.milestoning_scheme = first.milestoning_scheme
    return alias


def union_of_sets(class_name: str, sets: Sequence[RelationalClassMapping]) -> RelationalUnionMapping:
    """The class mapping that is the union of its sets: one query, with each row marked by the set it came from, and the same property mappings,
    on the columns of the union."""
    check_sets(class_name, sets)
    for each in sets:
        other = [pm.property.id for pm in each.property_mappings if not isinstance(pm.target, Column)]
        if other:
            raise ValueError(f"the set '{each.id}' of {class_name} maps {', '.join(other)} to something other than a column: the properties of a union are "
                             f"its columns, and a navigation between sets is said by an association that names them")
    alias = union_alias(f"{class_name.upper()}_SETS", [(_alias(each), each.id or "") for each in sets], f"{class_name}")
    by_name = {c.name: c for c in alias.columns if isinstance(c, Column)}
    mappings = [RelationalPropertyMapping(pm.property, by_name[pm.target.name], class_name) for pm in sets[0].property_mappings
                if isinstance(pm.target, Column)]
    return RelationalUnionMapping(sets[0].clazz, list(sets), mappings, milestone_mapping=sets[0].milestone_mapping)


def set_pair_condition(join: Join, source_set_id: str | None, target_set_id: str | None) -> Operation | None:
    """That the sets of a row of the source and a row of the target are the pair: ``source._set = 'a' AND target._set = 'b'``, with only the side that has
    sets; and what the join says besides."""
    def in_set(relation, set_id):
        column = next(c for c in relation.columns if isinstance(c, Column) and c.name == SET_COLUMN)
        return ComparisonOperation(column, ComparisonOperator.EQUAL, StringConstantOperation(set_id))
    terms: list[Operation] = []
    if source_set_id:
        terms.append(in_set(join.lhs.owner_relation, source_set_id))
    if target_set_id:
        terms.append(in_set(join.rhs.owner_relation, target_set_id))
    if join.condition is not None:
        terms.append(join.condition)
    if not terms:
        return None
    condition: Operation = terms[0]
    for term in terms[1:]:
        condition = LogicalOperation(condition, LogicalOperator.AND, term)
    return condition


def merge_set_navigations(class_mapping: ClassMapping) -> list[PropertyMapping]:
    """The property mappings with those of one navigation between sets made one, as the finder reads a navigation: a property mapped from one pair of sets to
    another for each pair of sets is one join, that holds for a row of the source and a row of the target if their sets
    are one of the pairs. The finder joins on one pair of key columns, so the pairs must all join on the same ones; a property mapped between no sets
    is as it was."""
    class_name, property_mappings = class_mapping.clazz.name, class_mapping.property_mappings

    def typed_by(pm: PropertyMapping) -> str:
        assert isinstance(pm.property.type, Class)  # a navigation is typed by a class
        return pm.property.type.name

    def between_sets(pm: PropertyMapping) -> bool:
        # from a set of this class, or to a set of another class: the target of a navigation to a class that is not a union is the class
        return isinstance(pm, NavigationPropertyMapping) and not isinstance(pm.target, EmbeddedSetMapping) and (
            pm.source_set_id != class_mapping.id or pm.target_set_id != typed_by(pm))

    merged: list[PropertyMapping] = []
    done: set[str] = set()
    for pm in property_mappings:
        if not between_sets(pm):
            merged.append(pm)
            continue
        if pm.property.id in done:
            continue
        done.add(pm.property.id)
        pairs = [other for other in property_mappings if other.property is pm.property and between_sets(other)]
        joins = [other.target for other in pairs]
        assert all(isinstance(j, Join) for j in joins)
        first = joins[0]
        if any((j.lhs.name, j.rhs.name, j.lhs.owner_relation, j.rhs.owner_relation) != (first.lhs.name, first.rhs.name, first.lhs.owner_relation, first.rhs.owner_relation)
               for j in joins):
            raise ValueError(f"the pairs of sets of the navigation {pm.property.id} of {class_name} join on different columns: "
                             f"the finder joins on one pair of columns, and then on the sets of the pairs")
        condition: Operation | None = None
        for other in pairs:
            assert isinstance(other, NavigationPropertyMapping)
            source_set = other.source_set_id if other.source_set_id != class_mapping.id else None  # the class mapping itself is not a set
            target_set = other.target_set_id if other.target_set_id != typed_by(other) else None  # nor is the class mapping that it reaches
            term = set_pair_condition(other.target, source_set, target_set)
            assert term is not None  # a pair has a set
            condition = term if condition is None else LogicalOperation(condition, LogicalOperator.OR, term)
        merged.append(RelationalNavigationPropertyMapping(pm.property, Join(first.lhs, first.rhs, conditions=[condition], condition_texts=[None]), class_mapping.id,
                                                          typed_by(pm)))
    return merged
