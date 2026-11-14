"""A query that a class is mapped to is an alias of a query expression: a SELECT, or the UNION ALL of the SELECTs of the sets of a class. A table, and an alias of a table, is not."""
from model.query_sets import union_alias
from model.relational import QuerySpecification, Table, TableAlias, UnionOperation, query_of
from model.sql_query import parse_query

_PEOPLE = Table("people", [("id", "INT"), ("name", "VARCHAR")])


def test_a_table_and_an_alias_of_a_table_are_not_queries():
    assert query_of(_PEOPLE) is None
    assert query_of(TableAlias(_PEOPLE, "p")) is None


def test_an_alias_of_a_select_is_a_query():
    select = parse_query("SELECT id, name FROM people", [_PEOPLE])
    assert isinstance(select, QuerySpecification)
    alias = TableAlias(select, "PEOPLE_Q", [("id", "INT"), ("name", "VARCHAR")])
    assert query_of(alias) is alias


def test_the_union_of_sets_is_a_table_alias_of_a_union_and_a_query():
    sets = [(TableAlias(parse_query("SELECT id, name FROM people", [_PEOPLE]), name, [("id", "INT"), ("name", "VARCHAR")]), name.lower())
            for name in ("A", "B")]
    union = union_alias("PEOPLE_SETS", sets, "People")
    assert type(union) is TableAlias and isinstance(union.relation, UnionOperation)
    assert query_of(union) is union
