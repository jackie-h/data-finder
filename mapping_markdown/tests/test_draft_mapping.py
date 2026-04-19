import os
import tempfile

import duckdb
import pytest

from mapping_markdown.markdown_mapping import draft_from_repository, load
from datafinder_duckdb.duckdb_reader import read_repository_from_duckdb
from model.relational import Repository, Schema, Table, Column


def _build_repository() -> Repository:
    repo = Repository("finance_db", "duckdb://test.db")
    ref_data = Schema("ref_data", repo)
    trading = Schema("trading", repo)
    Table("account_master", [Column("ID", "INT"), Column("ACCT_NAME", "VARCHAR")], ref_data)
    Table("price", [Column("SYM", "VARCHAR"), Column("PRICE", "DOUBLE"),
                    Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")], ref_data)
    Table("trades", [Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
                     Column("account_id", "INT"), Column("in_z", "TIMESTAMP"),
                     Column("out_z", "TIMESTAMP")], trading)
    return repo


class TestDraftFromRepository:

    def setup_method(self):
        self.repo = _build_repository()
        self.draft = draft_from_repository("Finance Draft", self.repo)

    def test_draft_has_title(self):
        assert "# Finance Draft" in self.draft

    def test_draft_has_repository(self):
        assert "## Repository: finance_db" in self.draft

    def test_draft_has_schemas(self):
        assert "### Schema: ref_data" in self.draft
        assert "### Schema: trading" in self.draft

    def test_draft_table_headings_use_placeholder(self):
        assert "#### Table: account_master → ?" in self.draft
        assert "#### Table: price → ?" in self.draft
        assert "#### Table: trades → ?" in self.draft

    def test_draft_has_column_names(self):
        assert "ID" in self.draft
        assert "ACCT_NAME" in self.draft
        assert "account_id" in self.draft

    def test_draft_has_column_types(self):
        assert "INT" in self.draft
        assert "VARCHAR" in self.draft
        assert "DOUBLE" in self.draft
        assert "TIMESTAMP" in self.draft

    def test_draft_property_column_is_empty(self):
        for line in self.draft.splitlines():
            if "|" in line and "Column" not in line and "---" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                if len(cells) == 3:
                    # last cell is Property — should be blank in draft rows
                    assert cells[2] == ""


class TestDuckDbReader:

    def setup_method(self):
        self.db_file = tempfile.mktemp(suffix=".db")
        conn = duckdb.connect(self.db_file)
        conn.execute("CREATE SCHEMA trading")
        conn.execute("CREATE TABLE trading.trades (sym VARCHAR, price DOUBLE, account_id INT)")
        conn.execute("CREATE SCHEMA ref_data")
        conn.execute("CREATE TABLE ref_data.account_master (id INT, name VARCHAR)")
        conn.close()

    def teardown_method(self):
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)
        wal = self.db_file + ".wal"
        if os.path.exists(wal):
            os.unlink(wal)

    def test_repository_name(self):
        repo = read_repository_from_duckdb(self.db_file, "test_repo")
        assert repo.name == "test_repo"

    def test_repository_location(self):
        repo = read_repository_from_duckdb(self.db_file)
        assert repo.location == f"duckdb://{self.db_file}"

    def test_schemas_discovered(self):
        repo = read_repository_from_duckdb(self.db_file)
        schema_names = {s.name for s in repo.schemas}
        assert "trading" in schema_names
        assert "ref_data" in schema_names

    def test_tables_discovered(self):
        repo = read_repository_from_duckdb(self.db_file)
        tables = {t.name for s in repo.schemas for t in s.tables}
        assert "trades" in tables
        assert "account_master" in tables

    def test_columns_discovered(self):
        repo = read_repository_from_duckdb(self.db_file)
        trades = next(t for s in repo.schemas for t in s.tables if t.name == "trades")
        col_names = [c.name for c in trades.columns]
        assert col_names == ["sym", "price", "account_id"]

    def test_column_types(self):
        repo = read_repository_from_duckdb(self.db_file)
        trades = next(t for s in repo.schemas for t in s.tables if t.name == "trades")
        col_types = {c.name: c.type for c in trades.columns}
        assert col_types["sym"] == "VARCHAR"
        assert col_types["price"] == "DOUBLE"
        assert col_types["account_id"] == "INTEGER"

    def test_draft_from_duckdb(self):
        repo = read_repository_from_duckdb(self.db_file, "finance_db")
        draft = draft_from_repository("Finance Draft", repo)
        assert "#### Table: trades → ?" in draft
        assert "#### Table: account_master → ?" in draft
        assert "VARCHAR" in draft
        assert "DOUBLE" in draft
