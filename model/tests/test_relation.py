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


def test_an_alias_of_a_table_has_columns_of_its_own_that_it_owns():
    alias = TableAlias(_TABLE, "a")
    columns = [c for c in alias.columns if isinstance(c, Column)]
    assert [(c.name, c.type) for c in columns] == [("ID", "INT"), ("NAME", "VARCHAR")]
    assert all(c.owner is alias and c is not original for c, original in zip(columns, _TABLE.columns))
    assert alias.column("ID", "INT") is alias.columns[0]
    made = alias.column("EXTRA", "DATE")  # a table in a finder lists only the columns it reads: the alias makes the rest
    assert made.owner is alias and alias.columns[-1] is made


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
    # until a table claims it, a column has no owner
    assert Column("ID", "INT").owner is None


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


def test_a_column_with_no_owner_says_so_when_its_owner_is_needed():
    import pytest
    orphan = Column("ID", "INT")
    with pytest.raises(ValueError, match="Column 'ID' has no owner"):
        orphan.owner_relation
    with pytest.raises(ValueError, match="no owner"):
        orphan.owner_name
    # a column of a table has one
    table = Table("accounts", [("NAME", "VARCHAR")])
    assert table.columns[0].owner_relation is table and table.columns[0].owner_name == "accounts"


def test_a_column_of_an_unnamed_relation_has_no_name_to_read_it_by():
    import pytest
    query = QuerySpecification([_NAME], NoOperation(), table=_TABLE)
    with pytest.raises(TypeError, match="has no name"):
        Column("name", "VARCHAR", query).owner_name


def test_a_table_is_made_of_names_and_types():
    table = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])
    assert [(c.name, c.type) for c in table.columns] == [("ID", "INT"), ("NAME", "VARCHAR")]
    assert all(c.owner is table and c.table is table for c in table.columns)
    # a column that is more than a name and a type is a Column, and the two can be mixed
    key = Column("ID", "INT", primary_key=True)
    mixed = Table("accounts", [key, ("NAME", "VARCHAR")])
    assert mixed.columns[0] is key and key.owner is mixed and mixed.columns[1].name == "NAME"
    assert Table("empty").columns == []


def test_add_column_adds_a_column_the_table_owns():
    table = Table("accounts", [("ID", "INT")])
    name = table.add_column("NAME", "VARCHAR")
    key = table.add_column("CODE", "VARCHAR", primary_key=True)
    assert [c.name for c in table.columns] == ["ID", "NAME", "CODE"]
    assert name.owner is table and name.table is table and name.nullable
    assert key.primary_key and not key.nullable
    assert table.add_column("AGE", "INT", nullable=False).nullable is False
    # a relation that reads the table sees its columns
    assert len(TableAlias(table, "a").columns) == 4


def test_a_column_name_is_used_once_in_a_table():
    import pytest
    with pytest.raises(ValueError, match="Duplicate column name 'ID' in table 'accounts'"):
        Table("accounts", [("ID", "INT"), ("ID", "INT")])
    table = Table("accounts", [("ID", "INT")])
    with pytest.raises(ValueError, match="Duplicate column name 'ID'"):
        table.add_column("ID", "INT")
    with pytest.raises(ValueError, match="Duplicate column name 'ID'"):
        Table("accounts", [("ID", "INT"), ("ID", "INT")])


def test_a_milestoned_table_is_made_of_names_and_types_too():
    from model.milestoning import MilestonedTable, ProcessingTemporalColumns
    milestoned = MilestonedTable("trades", [("ID", "INT")], ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"),
                                                                                       Column("out_z", "TIMESTAMP")))
    assert [c.name for c in milestoned.columns] == ["ID"] and milestoned.columns[0].owner is milestoned


def test_a_table_takes_its_primary_key_in_the_constructor():
    table = Table("accounts", [("ID", "INT"), ("REGION", "VARCHAR"), ("NAME", "VARCHAR")], primary_key=["ID", "REGION"])
    id_, region, name = table.columns
    assert id_.primary_key and region.primary_key and not name.primary_key
    # as in Legend, a primary key is never null
    assert not id_.nullable and not region.nullable and name.nullable
    assert table.primary_key == [id_, region]
    assert Table("none", [("ID", "INT")]).primary_key == []


def test_the_primary_key_can_name_columns_or_be_them():
    key = Column("ID", "INT")
    table = Table("accounts", [key, ("NAME", "VARCHAR")], primary_key=[key])
    assert key.primary_key and table.primary_key == [key]
    # a column already made a key stays one
    assert Table("accounts", [Column("ID", "INT", primary_key=True)], primary_key=["ID"]).primary_key[0].primary_key


def test_a_primary_key_is_a_column_of_the_table():
    import pytest
    with pytest.raises(ValueError, match="Primary key column 'MISSING' is not a column of table 'accounts'"):
        Table("accounts", [("ID", "INT")], primary_key=["MISSING"])
    with pytest.raises(ValueError, match="Primary key column 'ID' is not a column of table 'accounts'"):
        Table("accounts", [("ID", "INT")], primary_key=[Column("ID", "INT")])  # another column of that name


def test_a_milestoned_table_has_a_primary_key_too():
    from model.milestoning import MilestonedTable, ProcessingTemporalColumns
    milestoned = MilestonedTable("trades", [("ID", "INT"), ("SYM", "VARCHAR")],
                                 ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")),
                                 primary_key=["ID"])
    assert [c.name for c in milestoned.primary_key] == ["ID"]
    # add_column makes a key the same way
    table = Table("t")
    key = table.add_column("ID", "INT", primary_key=True)
    assert table.primary_key == [key] and not key.nullable
