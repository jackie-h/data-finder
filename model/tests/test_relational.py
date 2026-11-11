from model.relational import Database, DataCatalog, Schema, Table, Column


class TestDatabase:

    def test_schema_registered_in_repository(self):
        repo = Database("finance_db", "jdbc:duckdb://finance.db")
        schema = Schema("trading", repo)
        assert schema in repo.schemas

    def test_multiple_schemas_registered(self):
        repo = Database("finance_db")
        s1 = Schema("trading", repo)
        s2 = Schema("ref_data", repo)
        assert s1 in repo.schemas
        assert s2 in repo.schemas

    def test_schemas_order_preserved(self):
        repo = Database("finance_db")
        s1 = Schema("trading", repo)
        s2 = Schema("ref_data", repo)
        assert repo.schemas == [s1, s2]

    def test_empty_repository_has_no_schemas(self):
        repo = Database("empty_db")
        assert repo.schemas == []

    def test_none_repository_does_not_raise(self):
        schema = Schema("orphan", None)
        assert schema.datastore is None

    def test_repository_location(self):
        repo = Database("finance_db", "jdbc:duckdb://finance.db")
        assert repo.location == "jdbc:duckdb://finance.db"

    def test_schema_holds_repository_reference(self):
        repo = Database("finance_db")
        schema = Schema("trading", repo)
        assert schema.datastore is repo


class TestDataCatalog:

    def test_datacatalog_name(self):
        catalog = DataCatalog("my_catalog")
        assert catalog.name == "my_catalog"

    def test_datacatalog_namespace(self):
        assert DataCatalog("my_catalog").namespace("trading") == "my_catalog.trading"

    def test_database_namespace(self):
        assert Database("finance_db").namespace("trading") == "trading"


class TestSchema:

    def test_table_registered_in_schema(self):
        schema = Schema("trading")
        table = Table("trades", [], schema)
        assert table in schema.tables

    def test_multiple_tables_registered(self):
        schema = Schema("trading")
        t1 = Table("trades", [], schema)
        t2 = Table("accounts", [], schema)
        assert t1 in schema.tables
        assert t2 in schema.tables

    def test_tables_order_preserved(self):
        schema = Schema("trading")
        t1 = Table("trades", [], schema)
        t2 = Table("accounts", [], schema)
        assert schema.tables == [t1, t2]

    def test_empty_schema_has_no_tables(self):
        schema = Schema("empty")
        assert schema.tables == []

    def test_none_schema_does_not_raise(self):
        table = Table("orphan", [], None)
        assert table.schema is None

    def test_table_holds_schema_reference(self):
        schema = Schema("trading")
        table = Table("trades", [], schema)
        assert table.schema is schema

    def test_columns_assigned_to_table(self):
        schema = Schema("trading")
        col = Column("id", "INT")
        table = Table("trades", [col], schema)
        assert col.table is table



class TestQualifiedName:

    def test_no_schema(self):
        table = Table("trades", [])
        assert table.qualified_name == "trades"

    def test_schema_no_datastore(self):
        table = Table("trades", [], Schema("trading"))
        assert table.qualified_name == "trading.trades"

    def test_schema_with_database(self):
        table = Table("trades", [], Schema("trading", Database("db")))
        assert table.qualified_name == "trading.trades"

    def test_schema_with_datacatalog(self):
        table = Table("trades", [], Schema("trading", DataCatalog("my_catalog")))
        assert table.qualified_name == "my_catalog.trading.trades"


class TestColumnNullable:

    def test_nullable_by_default(self):
        assert Column("name", "VARCHAR").nullable is True

    def test_not_null(self):
        assert Column("name", "VARCHAR", nullable=False).nullable is False

    def test_primary_key_is_not_nullable(self):
        assert Column("id", "INTEGER", primary_key=True).nullable is False


# --- JoinOperation: a relation joined on a condition, of an ANSI join type ---------------------------------------------

import pytest
from model.relational import (ComparisonOperation, ComparisonOperator, JoinKind, JoinOperation, LogicalOperation,
                              LogicalOperator, Table as _JoinTable, TableAlias as _JoinAlias, Column as _JoinColumn)

_FIRMS = _JoinTable("firms", [("ID", "INT")], primary_key=["ID"])
_PEOPLE = _JoinTable("people", [("ID", "INT"), ("FIRMID", "INT"), ("MANAGERID", "INT")], primary_key=["ID"])


def _equal(left, right):
    return ComparisonOperation(left, ComparisonOperator.EQUAL, right)


def test_the_join_types_are_ansi_sqls():
    assert [k.name for k in JoinKind] == ["INNER", "LEFT", "RIGHT", "FULL"]


def test_a_join_is_a_target_a_condition_and_a_kind():
    condition = _equal(_FIRMS.columns[0], _PEOPLE.columns[1])
    join = JoinOperation("People", _PEOPLE, condition, JoinKind.LEFT)
    assert join.target is _PEOPLE and join.operation is condition and join.kind is JoinKind.LEFT
    assert join.table is _PEOPLE


def test_the_owner_of_a_column_says_which_side_it_is_on():
    join = JoinOperation("People", _PEOPLE, _equal(_FIRMS.columns[0], _PEOPLE.columns[1]), JoinKind.INNER)
    assert [join.is_target(c) for c in join.columns()] == [False, True]
    assert join.keys() == (_FIRMS.columns[0], _PEOPLE.columns[1])
    # the written order does not matter: the key still reads (from what is joined from, of the target)
    reversed_join = JoinOperation("People", _PEOPLE, _equal(_PEOPLE.columns[1], _FIRMS.columns[0]), JoinKind.INNER)
    assert reversed_join.keys() == (_FIRMS.columns[0], _PEOPLE.columns[1])


def test_a_condition_can_be_more_than_one_column_equal_to_another():
    condition = LogicalOperation(_equal(_FIRMS.columns[0], _PEOPLE.columns[1]), LogicalOperator.AND,
                                 ComparisonOperation(_PEOPLE.columns[0], ComparisonOperator.GREATER_THAN, _FIRMS.columns[0]))
    join = JoinOperation("People", _PEOPLE, condition, JoinKind.INNER)
    assert len(join.columns()) == 4
    with pytest.raises(ValueError, match="no key"):
        join.keys()


def test_a_column_with_no_owner_is_refused_when_the_join_is_made():
    with pytest.raises(ValueError, match="no owner"):
        JoinOperation("People", _PEOPLE, _equal(_JoinColumn("ID", "INT"), _PEOPLE.columns[1]), JoinKind.INNER)


def test_one_table_on_both_sides_is_joined_to_an_alias_of_it_that_owns_its_columns():
    manager_id = _PEOPLE.columns[2]
    join = JoinOperation.on_keys("manager", _PEOPLE, manager_id, _PEOPLE.columns[0], JoinKind.LEFT)
    assert isinstance(join.target, _JoinAlias) and join.target.name == "manager" and join.table is _PEOPLE
    source, target = join.keys()
    assert source is manager_id and target.owner is join.target and target.name == "ID"
    assert [join.is_target(c) for c in (source, target)] == [False, True]


def test_a_different_table_is_joined_as_it_is():
    join = JoinOperation.on_keys("firm", _FIRMS, _PEOPLE.columns[1], _FIRMS.columns[0], JoinKind.LEFT)
    assert join.target is _FIRMS
