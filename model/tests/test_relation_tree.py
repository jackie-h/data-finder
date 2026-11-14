"""A tree of relations is what was written of a query, in the order it was written. It says nothing of SELECTs."""
import dataclasses

import pytest

from model.relation_tree import (
    Aggregate, Concatenate, Distinct, Drop, Extend, Filter, Join, Project, Read, Select, Sort, Take, sources_of)
from model.relational import JoinKind, NoOperation, Table

_TABLE = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])


def _read() -> Read:
    return Read([], _TABLE)


def test_a_tree_starts_at_the_rows_a_finder_reads():
    read = _read()
    assert read.table is _TABLE and isinstance(read.op, NoOperation) and read.group_by == () and read.having is None
    assert sources_of(read) == ()


def test_the_order_steps_are_written_in_is_the_shape_of_the_tree():
    read = _read()
    sorted_then_taken = Take(Sort(read, ()), 3)
    taken_then_sorted = Sort(Take(read, 3), ())
    assert sources_of(sorted_then_taken) == (sorted_then_taken.source,) and isinstance(sorted_then_taken.source, Sort)
    assert isinstance(taken_then_sorted.source, Take)
    assert sources_of(sources_of(sorted_then_taken)[0]) == (read,)


def test_a_join_and_a_concatenate_are_of_two_nodes():
    left, right = _read(), _read()
    assert sources_of(Join(left, right, JoinKind.LEFT, NoOperation(), [])) == (left, right)
    assert sources_of(Concatenate(left, right, all=True)) == (left, right)


def test_every_other_step_is_of_the_one_node_before_it():
    read = _read()
    for node in (Filter(read, []), Extend(read, []), Select(read, []), Aggregate(read, []), Distinct(read), Sort(read, ()), Drop(read, 1), Take(read, 1)):
        assert sources_of(node) == (read,)
    assert all(issubclass(kind, Project) for kind in (Filter, Extend, Select, Aggregate))


def test_a_node_never_changes():
    node = Take(_read(), 3)
    with pytest.raises(dataclasses.FrozenInstanceError):
        node.count = 5  # type: ignore[misc]
    assert dataclasses.replace(node, count=5).count == 5 and node.count == 3


def test_two_nodes_are_the_same_only_when_they_are_the_same_object():
    read = _read()
    assert Take(read, 3) is not Take(read, 3) and Take(read, 3) != Take(read, 3)
    node = Take(read, 3)
    assert node == node and len({node, Take(read, 3)}) == 2
