from model.relational import Repository, Schema, Table, Column


class TestRepository:

    def test_schema_registered_in_repository(self):
        repo = Repository("finance_db", "jdbc:duckdb://finance.db")
        schema = Schema("trading", repo)
        assert schema in repo.schemas

    def test_multiple_schemas_registered(self):
        repo = Repository("finance_db")
        s1 = Schema("trading", repo)
        s2 = Schema("ref_data", repo)
        assert s1 in repo.schemas
        assert s2 in repo.schemas

    def test_schemas_order_preserved(self):
        repo = Repository("finance_db")
        s1 = Schema("trading", repo)
        s2 = Schema("ref_data", repo)
        assert repo.schemas == [s1, s2]

    def test_empty_repository_has_no_schemas(self):
        repo = Repository("empty_db")
        assert repo.schemas == []

    def test_none_repository_does_not_raise(self):
        schema = Schema("orphan", None)
        assert schema.repository is None

    def test_repository_location(self):
        repo = Repository("finance_db", "jdbc:duckdb://finance.db")
        assert repo.location == "jdbc:duckdb://finance.db"

    def test_schema_holds_repository_reference(self):
        repo = Repository("finance_db")
        schema = Schema("trading", repo)
        assert schema.repository is repo


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
