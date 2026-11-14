"""What was written of a query, as a tree of relations.

Each step a person writes of a result (``filter``, ``order_by``, ``limit``, ``join`` ...) is a node that has the node it was made
from, and the order they are written in is the shape of the tree. A tree says nothing of SELECTs: SQL says some steps in one clause
and others need a query of their own, which is decided when the tree is lowered (see ``datafinder.relation_planner``), not when it
is written. A node never changes, so a step of a result leaves the result as it was.

``Read`` is where a tree starts: the rows of a table as a finder asks for them, with its columns, dates and filter. Everything else
is of the rows of the node before it, or of two of them.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Sequence

from model.relational import JoinKind, NoOperation, Operation, Relation, SortOperation


@dataclass(frozen=True, eq=False)
class RelationNode:
    """A relation that is worked out from others. Two nodes are the same only when they are the same object: a result used twice in a
    query is one node, and is read as two relations."""


@dataclass(frozen=True, eq=False)
class Read(RelationNode):
    """The rows of ``table`` as a finder asks for them: ``columns`` shown, those where ``op`` holds, at the dates given. The aggregates
    in ``columns`` are grouped by ``group_by`` and those groups are kept by ``having``, which a finder says of the rows it reads."""
    columns: Sequence
    table: Relation
    op: Operation = field(default_factory=NoOperation)
    business_date: datetime.date | None = None
    processing_datetime: datetime.datetime | datetime.date | None = None
    business_date_to: datetime.date | None = None
    processing_datetime_to: datetime.datetime | datetime.date | None = None
    group_by: tuple = ()
    having: Operation | None = None


@dataclass(frozen=True, eq=False)
class Project(RelationNode):
    """A query over the rows of ``source``: the ``columns`` shown of those where ``op`` holds, and grouped by ``group_by`` if it has any."""
    source: RelationNode
    columns: Sequence
    op: Operation | None = None
    group_by: tuple = ()


class Filter(Project):
    """The rows of ``source`` for which a condition holds."""


class Extend(Project):
    """The columns of ``source`` and more that are worked out from them."""


class Select(Project):
    """Some of the columns of ``source``."""


class Aggregate(Project):
    """The grouping ``keys`` of ``source`` and an aggregate of the rows of each group."""


@dataclass(frozen=True, eq=False)
class Join(RelationNode):
    """The rows of ``left`` put together with those of ``right`` where ``condition`` holds, as ``kind`` says. ``columns`` are the
    columns of the two, each named, which the condition reads from."""
    left: RelationNode
    right: RelationNode
    kind: JoinKind
    condition: Operation
    columns: Sequence


@dataclass(frozen=True, eq=False)
class Concatenate(RelationNode):
    """The rows of ``left`` and of ``right``, which have the same columns: each distinct row once, or every row if ``all``."""
    left: RelationNode
    right: RelationNode
    all: bool = False


@dataclass(frozen=True, eq=False)
class Distinct(RelationNode):
    """Each distinct row of ``source`` once."""
    source: RelationNode


@dataclass(frozen=True, eq=False)
class Sort(RelationNode):
    """The rows of ``source`` in the order of ``sorts``. A sort of a column the result has already been sorted by is the one that
    counts, and the earlier ones order the rows it leaves tied."""
    source: RelationNode
    sorts: tuple[SortOperation, ...]


@dataclass(frozen=True, eq=False)
class Drop(RelationNode):
    """The rows of ``source`` after the first ``count`` of them."""
    source: RelationNode
    count: int


@dataclass(frozen=True, eq=False)
class Take(RelationNode):
    """The first ``count`` rows of ``source``."""
    source: RelationNode
    count: int


def sources_of(node: RelationNode) -> tuple[RelationNode, ...]:
    """The nodes a node is worked out from, in the order they were written."""
    if isinstance(node, (Join, Concatenate)):
        return node.left, node.right
    source = getattr(node, "source", None)
    return () if source is None else (source,)
