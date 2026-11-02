"""The benchmark cases: the checked-in expected rows are what the report's SQL returns on the generated data, and the
temporal clauses of the report's SQL are translated as SQL:2011 defines them."""
import pytest

from datafinder_examples_tests.tpch_bih_build import CASES_FILE, build
from datafinder_examples_tests.tpch_bih_definitions import DEFINITIONS
from datafinder_examples_tests.tpch_bih_queries import QUERIES
from datafinder_examples_tests.tpch_bih_sql import temporal_clauses_to_subselects, to_duckdb


def test_the_checked_in_cases_are_what_the_definitions_give():
    assert CASES_FILE.read_text() == build(), "run: uv run datafinder_examples/tools/build_tpch_bih_cases.py"


def test_every_case_has_a_finder_query_or_a_reason():
    assert sorted(QUERIES) == sorted(d.id for d in DEFINITIONS)


def test_a_case_id_is_used_once():
    ids = [d.id for d in DEFINITIONS]
    assert len(ids) == len(set(ids))


def _where(sql: str) -> str:
    return temporal_clauses_to_subselects(sql)


def test_as_of_is_begin_inclusive_and_end_exclusive():
    sql = _where("SELECT * FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '2005-06-01' FOR BUSINESS_TIME AS OF '1996-06-01' as p")
    assert "sys_time_begin <= '2005-06-01' AND sys_time_end > '2005-06-01'" in sql
    assert "validity_time_begin <= '1996-06-01' AND validity_time_end > '1996-06-01'" in sql
    assert sql.endswith(") AS p")


def test_from_to_is_the_period_begin_inclusive_and_end_exclusive():
    sql = _where("SELECT * FROM customer FOR BUSINESS_TIME FROM '1993-01-01' TO '2004-01-01' WHERE c_custkey = 1")
    assert "visible_time_begin < '2004-01-01' AND visible_time_end > '1993-01-01'" in sql
    assert sql.endswith("WHERE c_custkey = 1")


def test_between_includes_its_end():
    sql = _where("SELECT * FROM orders FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30'")
    assert "sys_time_begin <= '9999-12-30' AND sys_time_end > '0001-01-01'" in sql


def test_no_system_clause_reads_the_current_version_and_no_business_clause_reads_all_of_application_time():
    sql = _where("SELECT * FROM partsupp as ps1, partsupp as ps2")
    assert sql.count("sys_time_end > TIMESTAMP '2010-01-01 00:00:00'") == 2
    assert "validity_time" not in sql


def test_a_clause_in_a_subquery_is_translated_too():
    sql = _where("SELECT * FROM customer WHERE a = (SELECT max(b) FROM customer FOR BUSINESS_TIME AS OF '2000-01-01' WHERE c = 1)")
    assert sql.count("(SELECT * FROM customer") == 2 and "visible_time_begin <= '2000-01-01'" in sql


def test_the_reports_start_columns_are_the_repositorys_begin_columns():
    sql = to_duckdb("SELECT ps1.validity_time_start, ps1. VALIDITY_TIME_END FROM partsupp as ps1")
    assert "validity_time_begin" in sql and "_start" not in sql


def test_a_table_is_read_in_its_schema():
    assert "FROM tpch.orders" in to_duckdb("SELECT 1 FROM orders", schema="tpch")


@pytest.mark.parametrize("keyword", ["WHERE", "ORDER BY", "GROUP BY"])
def test_a_keyword_after_a_table_is_not_taken_for_its_alias(keyword):
    assert f") {keyword}" in _where(f"SELECT * FROM orders FOR BUSINESS_TIME AS OF '1996-01-01' {keyword} o_orderkey")
