"""An attribute's column belongs to a relation, which is given as the relation: a name does not stand for one."""
import pytest

from datafinder import IntegerAttribute, StringAttribute, to_sql
from model.relational import Column, NoOperation, Table, TableAlias

_TABLE = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])


def test_an_attribute_takes_the_relation_that_owns_its_column():
    attribute = StringAttribute("Name", "NAME", "VARCHAR", _TABLE)
    assert attribute.owner() is _TABLE and attribute.column().owner is _TABLE
    assert to_sql(None, None, [attribute], _TABLE, NoOperation()) == 'SELECT t0.NAME AS "Name" FROM accounts AS t0'


def test_an_attribute_cannot_be_given_a_name_as_its_owner():
    with pytest.raises(TypeError, match="owned by a Relation"):
        StringAttribute("Name", "NAME", "VARCHAR", "accounts")  # type: ignore[arg-type]


def test_attributes_of_tables_with_the_same_name_share_an_alias():
    # the finders name the table they read with a Table of its own, which is the same relation as the root by its name
    other = Table("accounts", [])
    sql = to_sql(None, None, [IntegerAttribute("Id", "ID", "INT", _TABLE), StringAttribute("Name", "NAME", "VARCHAR", other)],
                 _TABLE, NoOperation())
    assert sql == 'SELECT t0.ID AS "Id",t0.NAME AS "Name" FROM accounts AS t0'


def test_a_table_is_written_by_its_qualified_name():
    from model.relational import Database, Schema
    schema = Schema("vt", Database("db"))
    table = Table("trades", [("ID", "INT")], schema)
    attribute = IntegerAttribute("Id", "ID", "INT", table)
    assert to_sql(None, None, [attribute], table, NoOperation()) == 'SELECT t0.ID AS "Id" FROM vt.trades AS t0'


def test_the_generator_names_each_relation_it_reads_with_a_table_alias():
    from datafinder.sql_generator import SQLQueryGenerator, build_query_specification
    query = build_query_specification(None, None, [StringAttribute("Name", "NAME", "VARCHAR", _TABLE)], _TABLE, NoOperation())
    generator = SQLQueryGenerator()
    generator.generate(query)
    assert all(isinstance(alias, TableAlias) for alias in generator._from)
    assert [(alias.name, alias.relation.name) for alias in generator._from] == [("t0", "accounts")]  # type: ignore[attr-defined]


def test_reading_a_column_that_belongs_to_no_relation_is_a_clear_error():
    orphan = Column("ID", "INT")
    with pytest.raises(ValueError, match="Column 'ID' has no owner"):
        to_sql(None, None, [orphan], _TABLE, NoOperation())
