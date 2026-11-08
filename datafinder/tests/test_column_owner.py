"""An attribute's column belongs to a relation, which can be given as the relation or, as the finders do, by name."""
from datafinder import IntegerAttribute, StringAttribute, to_sql
from model.relational import Column, NoOperation, Relation, Table, TableAlias

_TABLE = Table("accounts", [Column("ID", "INT"), Column("NAME", "VARCHAR")])


def test_an_attribute_owned_by_a_table_object_reads_as_one_owned_by_its_name():
    by_table = StringAttribute("Name", "NAME", "VARCHAR", _TABLE)
    by_name = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    assert by_table.owner() is _TABLE and isinstance(by_name.owner(), Relation)
    assert by_table.column().owner is _TABLE and by_name.column().owner_name == "accounts"
    for attribute in (by_table, by_name):
        assert to_sql(None, None, [attribute], _TABLE, NoOperation()) == 'SELECT t0.NAME AS "Name" FROM accounts AS t0'


def test_two_attributes_of_one_table_share_its_alias_whichever_way_they_name_it():
    by_table = IntegerAttribute("Id", "ID", "INT", _TABLE)
    by_name = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    sql = to_sql(None, None, [by_table, by_name], _TABLE, NoOperation())
    assert sql == 'SELECT t0.ID AS "Id",t0.NAME AS "Name" FROM accounts AS t0'


def test_the_generator_names_each_relation_it_reads_with_a_table_alias():
    from datafinder.sql_generator import SQLQueryGenerator, build_query_specification
    query = build_query_specification(None, None, [StringAttribute("Name", "NAME", "VARCHAR", _TABLE)], _TABLE, NoOperation())
    generator = SQLQueryGenerator()
    generator.generate(query)
    assert all(isinstance(alias, TableAlias) for alias in generator._from)
    assert [(alias.name, alias.relation.name) for alias in generator._from] == [("t0", "accounts")]  # type: ignore[attr-defined]
