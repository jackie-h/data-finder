"""Relations, as in Legend: a relation has columns, a named relation has a name, and a table alias is a relation under a name."""
from model.relational import (
    Alias, Column, NamedRelation, NoOperation, OrderedQuerySpecification, QueryExpression, QuerySpecification, Relation,
    Table, TableAlias)
from model.view import View
from model.relational import Table as _OwnerTable
_OWNER_ACCOUNTS = _OwnerTable('accounts', [])

_ID = Column("ID", "INT", _OWNER_ACCOUNTS)
_NAME = Column("NAME", "VARCHAR", _OWNER_ACCOUNTS)
_TABLE = Table("accounts", [_ID, _NAME])


def test_a_table_and_a_view_are_named_relations():
    view = View("v", "s", _TABLE, [Alias(_NAME, "name")])
    for named in (_TABLE, view):
        assert isinstance(named, NamedRelation) and isinstance(named, Relation)
    assert _TABLE.name == "accounts" and view.name == "v"


def test_a_query_is_a_relation_that_has_no_name():
    select = QuerySpecification([_NAME, _ID], NoOperation(), table=_TABLE)
    ordered = OrderedQuerySpecification([_NAME], NoOperation(), table=_TABLE)
    for query in (select, ordered):
        assert isinstance(query, QueryExpression) and isinstance(query, Relation)
        assert not isinstance(query, NamedRelation)


def test_the_columns_of_each_relation():
    # a table's Columns, in the order it was given them
    assert list(_TABLE.columns) == [_ID, _NAME]
    # a query's display list
    assert list(QuerySpecification([_NAME, _ID], NoOperation(), table=_TABLE).columns) == [_NAME, _ID]
    # a view's projection, then the milestoning columns of its root
    view = View("v", "s", _TABLE, [Alias(_NAME, "name")])
    assert [c.name for c in view.columns if isinstance(c, Alias)] == ["name"]
    # a relation is given its columns
    assert list(Relation([_ID]).columns) == [_ID]


def test_a_table_alias_is_a_named_relation_with_the_columns_of_what_it_names():
    query = QuerySpecification([_NAME], NoOperation(), table=_TABLE)
    alias = TableAlias(query, "relation1")
    assert isinstance(alias, NamedRelation) and alias.name == "relation1" and alias.relation is query
    assert list(alias.columns) == [_NAME]
    # a table can be read under a name too
    assert list(TableAlias(_TABLE, "a").columns) == [_ID, _NAME]


def test_the_same_relation_can_be_read_under_two_names():
    query = QuerySpecification([_NAME], NoOperation(), table=_TABLE)
    first, second = TableAlias(query, "one"), TableAlias(query, "two")
    assert (first.name, second.name) == ("one", "two") and first.relation is second.relation


def test_a_columns_owner_is_a_relation():
    owner = Table("accounts", [])
    assert Column("ID", "INT", owner).owner is owner
    assert Column("ID", "INT", owner).owner_name == "accounts"
    # a query read under a name is owned by the alias
    alias = TableAlias(QuerySpecification([_NAME], NoOperation(), table=_TABLE), "relation1")
    assert Column("name", "VARCHAR", alias).owner_name == "relation1"
    # no owner, no name
    assert Column("ID", "INT").owner is None and Column("ID", "INT").owner_name is None


def test_a_name_is_not_an_owner():
    import pytest
    with pytest.raises(TypeError, match="owned by a Relation"):
        Column("ID", "INT", "accounts")  # type: ignore[arg-type]


def test_a_table_is_the_owner_of_its_columns():
    first, second = Column("ID", "INT"), Column("NAME", "VARCHAR")
    table = Table("accounts", [first, second])
    assert first.owner is table and second.owner is table and first.table is table
    # a milestoned table owns its milestoning columns too
    from model.milestoning import MilestonedTable, ProcessingTemporalColumns
    start, end = Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")
    milestoned = MilestonedTable("trades", [], ProcessingTemporalColumns(start, end))
    assert start.owner is milestoned and end.owner is milestoned
