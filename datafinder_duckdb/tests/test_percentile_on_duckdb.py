"""A percentile gives the values the definition gives, on DuckDB."""
import duckdb
import pytest

from datafinder import DoubleAttribute, IntegerAttribute, sql_for_dialect, to_sql
from model.relational import NoOperation, Table

_T = Table("pt", [("QTY", "INT"), ("PRICE", "DOUBLE"), ("G", "INT")])
_QTY = IntegerAttribute("Qty", "QTY", "INT", _T)
_PRICE = DoubleAttribute("Price", "PRICE", "DOUBLE", _T)
_G = IntegerAttribute("G", "G", "INT", _T)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE pt(QTY INT, PRICE DOUBLE, G INT)")
    c.execute("INSERT INTO pt VALUES (1, 1.5, 1), (2, 2.5, 1), (3, 3.5, 1), (4, 4.5, 1), (NULL, NULL, 1), (10, 0.5, 2)")
    return c


def _rows(con, *columns, group_by=()):
    sql = to_sql(None, None, list(columns), _T, NoOperation(), group_by=list(group_by))
    return con.sql(sql_for_dialect(sql, "duckdb") + (" ORDER BY 1" if group_by else "")).fetchall()


def test_the_median_interpolates_between_the_two_middle_values(con):
    assert _rows(con, _G, _QTY.percentile(0.5), _PRICE.percentile(0.5), group_by=[_G]) == [(1, 2.5, 3.0), (2, 10.0, 0.5)]


def test_a_null_is_not_one_of_the_values(con):
    assert _rows(con, _QTY.percentile(0.0), _QTY.percentile(1.0)) == [(1.0, 10.0)]


def test_a_discrete_percentile_is_a_value_that_is_there_of_the_type_of_the_column(con):
    [(low, high)] = _rows(con, _QTY.percentile(0.5, continuous=False), _QTY.percentile(0.9, continuous=False))
    assert (low, high) == (3, 10) and isinstance(low, int)


def test_reading_from_the_largest_value_mirrors_the_fraction(con):
    [(top, bottom)] = _rows(con, _QTY.percentile(0.25, ascending=False), _QTY.percentile(0.75))
    assert top == bottom == 4.0  # the value a quarter of the way down from the top is three quarters of the way up


def test_the_percentile_of_an_expression(con):
    [(value,)] = _rows(con, (_QTY * 2).percentile(0.5))
    assert value == 6.0
