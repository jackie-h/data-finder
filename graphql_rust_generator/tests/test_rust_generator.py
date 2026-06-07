"""
Tests for the Rust GraphQL generator — postgres and databricks backends.

Verifies that the generator produces structurally correct Rust source files
from the finance relational mapping. Does not compile the output (Rust toolchain
not required); checks that key identifiers, types, SQL fragments and
backend-specific patterns are present.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from graphql_rust_generator.generator import generate, _to_snake, _resolver_name

from mapping_markdown.markdown_mapping import load
from model.relational import Database, Schema, Table, Column

_FINANCE_MAPPING = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_mapping.md")
)


def _build_repo() -> Database:
    repo      = Database("finance_db", "duckdb://test.db")
    ref_data  = Schema("ref_data", repo)
    trading   = Schema("trading", repo)
    Table("account_master", [
        Column("ID", "INT", primary_key=True), Column("ACCT_NAME", "VARCHAR"),
    ], ref_data)
    Table("price", [
        Column("SYM", "VARCHAR", primary_key=True), Column("PRICE", "DOUBLE"),
        Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"),
    ], ref_data)
    Table("trades", [
        Column("sym", "VARCHAR"), Column("price", "DOUBLE"),
        Column("is_settled", "BOOLEAN"), Column("account_id", "INT"),
        Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"),
    ], trading)
    Table("contractualposition", [
        Column("DATE", "DATE"), Column("QUANTITY", "DOUBLE"), Column("NPV", "DOUBLE"),
        Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP"),
    ], trading)
    return repo


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg_out(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("pg_out")
    mapping = load(_FINANCE_MAPPING, _build_repo())
    generate(mapping, str(tmp), project_name="finance-api", backend="postgres")
    return tmp


@pytest.fixture(scope="module")
def db_out(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("db_out")
    mapping = load(_FINANCE_MAPPING, _build_repo())
    generate(mapping, str(tmp), project_name="finance-api-db", backend="databricks")
    return tmp


# ---------------------------------------------------------------------------
# Postgres backend
# ---------------------------------------------------------------------------

class TestPostgresFiles:
    def test_cargo_toml_exists(self, pg_out): assert (pg_out / "Cargo.toml").exists()
    def test_main_rs_exists(self, pg_out):    assert (pg_out / "src" / "main.rs").exists()
    def test_schema_rs_exists(self, pg_out):  assert (pg_out / "src" / "schema.rs").exists()


class TestPostgresCargo:
    def test_project_name(self, pg_out):
        assert 'name = "finance-api"' in (pg_out / "Cargo.toml").read_text()

    def test_sqlx_dep(self, pg_out):
        assert "sqlx" in (pg_out / "Cargo.toml").read_text()

    def test_no_reqwest(self, pg_out):
        assert "reqwest" not in (pg_out / "Cargo.toml").read_text()

    def test_async_graphql(self, pg_out):
        assert "async-graphql" in (pg_out / "Cargo.toml").read_text()


class TestPostgresMain:
    def test_pg_pool(self, pg_out):
        assert "PgPoolOptions" in (pg_out / "src" / "main.rs").read_text()

    def test_database_url(self, pg_out):
        assert "DATABASE_URL" in (pg_out / "src" / "main.rs").read_text()

    def test_graphiql(self, pg_out):
        assert "GraphiQL" in (pg_out / "src" / "main.rs").read_text()

    def test_tokio_main(self, pg_out):
        assert "#[tokio::main]" in (pg_out / "src" / "main.rs").read_text()


class TestPostgresSchema:
    @pytest.fixture(autouse=True)
    def _schema(self, pg_out):
        self.s = (pg_out / "src" / "schema.rs").read_text()

    def test_structs_present(self):
        for name in ("Account", "Instrument", "Trade", "ContractualPosition"):
            assert f"pub struct {name}" in self.s

    def test_simple_object_derive(self):
        assert "SimpleObject" in self.s

    def test_from_row_derive(self):
        assert "sqlx::FromRow" in self.s

    def test_query_root(self):       assert "pub struct QueryRoot" in self.s
    def test_accounts_resolver(self): assert "async fn accounts" in self.s
    def test_instruments_resolver(self): assert "async fn instruments" in self.s
    def test_trades_resolver(self):  assert "async fn trades" in self.s
    def test_cp_resolver(self):      assert "async fn contractual_positions" in self.s

    def test_positional_params(self):
        assert "$1" in self.s            # postgres parameter style

    def test_no_named_params(self):
        assert ":as_of" not in self.s    # named params are databricks only

    def test_fk_field_skipped(self):
        assert "#[graphql(skip)]" in self.s
        assert "account_fk" in self.s

    def test_complex_object_for_trade(self):
        assert "#[graphql(complex)]" in self.s
        assert "ComplexObject" in self.s

    def test_account_nav_resolver(self):
        assert "async fn account" in self.s
        assert "account_master" in self.s

    def test_as_of_arg(self):
        assert "as_of: Option<String>" in self.s

    def test_business_date_arg(self):
        assert "business_date: Option<String>" in self.s

    def test_processing_helper(self):
        assert "query_processing" in self.s

    def test_bitemporal_helper(self):
        assert "query_bitemporal" in self.s

    def test_correct_tables(self):
        assert "ref_data.account_master" in self.s
        assert "ref_data.price" in self.s
        assert "trading.trades" in self.s
        assert "trading.contractualposition" in self.s

    def test_nullable_valid_to(self):
        assert "Option<chrono::NaiveDateTime>" in self.s


# ---------------------------------------------------------------------------
# Databricks backend
# ---------------------------------------------------------------------------

class TestDatabricksFiles:
    def test_cargo_toml_exists(self, db_out): assert (db_out / "Cargo.toml").exists()
    def test_main_rs_exists(self, db_out):    assert (db_out / "src" / "main.rs").exists()
    def test_schema_rs_exists(self, db_out):  assert (db_out / "src" / "schema.rs").exists()


class TestDatabricksCargo:
    def test_project_name(self, db_out):
        assert 'name = "finance-api-db"' in (db_out / "Cargo.toml").read_text()

    def test_reqwest_dep(self, db_out):
        cargo = (db_out / "Cargo.toml").read_text()
        assert "reqwest" in cargo
        assert '"json"' in cargo

    def test_serde_json_dep(self, db_out):
        assert "serde_json" in (db_out / "Cargo.toml").read_text()

    def test_no_sqlx(self, db_out):
        assert "sqlx" not in (db_out / "Cargo.toml").read_text()

    def test_async_graphql(self, db_out):
        assert "async-graphql" in (db_out / "Cargo.toml").read_text()


class TestDatabricksMain:
    def test_databricks_client_struct(self, db_out):
        assert "pub struct DatabricksClient" in (db_out / "src" / "main.rs").read_text()

    def test_env_vars(self, db_out):
        main = (db_out / "src" / "main.rs").read_text()
        assert "DATABRICKS_HOST" in main
        assert "DATABRICKS_WAREHOUSE_ID" in main
        assert "DATABRICKS_TOKEN" in main

    def test_from_env(self, db_out):
        assert "from_env" in (db_out / "src" / "main.rs").read_text()

    def test_graphiql(self, db_out):
        assert "GraphiQL" in (db_out / "src" / "main.rs").read_text()

    def test_no_pg_pool(self, db_out):
        assert "PgPool" not in (db_out / "src" / "main.rs").read_text()


class TestDatabricksSchema:
    @pytest.fixture(autouse=True)
    def _schema(self, db_out):
        self.s = (db_out / "src" / "schema.rs").read_text()

    def test_structs_present(self):
        for name in ("Account", "Instrument", "Trade", "ContractualPosition"):
            assert f"pub struct {name}" in self.s

    def test_from_row_trait(self):
        assert "trait FromRow" in self.s

    def test_from_row_impls(self):
        for name in ("Account", "Instrument", "Trade", "ContractualPosition"):
            assert f"impl FromRow for {name}" in self.s

    def test_no_sqlx_from_row(self):
        assert "sqlx::FromRow" not in self.s

    def test_databricks_sql_helper(self):
        assert "async fn databricks_sql" in self.s

    def test_run_query_helper(self):
        assert "async fn run_query" in self.s

    def test_stmt_api_endpoint(self):
        assert "/api/2.0/sql/statements" in self.s

    def test_bearer_auth(self):
        assert "bearer_auth" in self.s

    def test_json_array_format(self):
        assert "JSON_ARRAY" in self.s

    def test_named_params(self):
        assert ":as_of" in self.s         # Databricks named parameter style

    def test_no_positional_params(self):
        # $1 only appears in postgres; for Databricks FK lookup we use :pk
        assert "$1" not in self.s

    def test_db_param_helper(self):
        assert "fn db_param" in self.s

    def test_json_coercions_present(self):
        for fn_name in ("as_i32", "as_f64", "as_bool", "as_string",
                        "as_naive_date", "as_naive_dt", "as_opt_naive_dt"):
            assert f"fn {fn_name}" in self.s

    def test_row_indexed_deserialization(self):
        assert "row[0]" in self.s

    def test_fk_field_skipped(self):
        assert "#[graphql(skip)]" in self.s
        assert "account_fk" in self.s

    def test_account_nav_resolver(self):
        assert "async fn account" in self.s
        assert "account_master" in self.s

    def test_named_fk_param(self):
        assert ":pk" in self.s

    def test_as_of_arg(self):
        assert "as_of: Option<String>" in self.s

    def test_business_date_arg(self):
        assert "business_date: Option<String>" in self.s

    def test_processing_helper(self):
        assert "async fn query_processing" in self.s

    def test_bitemporal_helper(self):
        assert "async fn query_bitemporal" in self.s

    def test_correct_tables(self):
        assert "ref_data.account_master" in self.s
        assert "trading.trades" in self.s

    def test_warehouse_id_in_request(self):
        assert "warehouse_id" in self.s

    def test_error_state_check(self):
        assert "SUCCEEDED" in self.s

    def test_data_array_extraction(self):
        assert "data_array" in self.s


# ---------------------------------------------------------------------------
# Generator utility functions
# ---------------------------------------------------------------------------

class TestGeneratorUtils:
    def test_to_snake_camel(self):   assert _to_snake("isSettled") == "is_settled"
    def test_to_snake_pascal(self):  assert _to_snake("ContractualPosition") == "contractual_position"
    def test_to_snake_single(self):  assert _to_snake("price") == "price"
    def test_resolver_name(self):
        assert _resolver_name("Account") == "accounts"
        assert _resolver_name("Trade") == "trades"
        assert _resolver_name("ContractualPosition") == "contractual_positions"

    def test_invalid_backend(self):
        import tempfile
        mapping = load(_FINANCE_MAPPING, _build_repo())
        with pytest.raises(ValueError, match="Unknown backend"):
            generate(mapping, tempfile.mkdtemp(), backend="oracle")
