import duckdb
import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import to_sql, StringAttribute, IntegerAttribute
from model.relational import Table, Column


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders (id INTEGER, notes VARCHAR, quantity INTEGER)"
    )
    conn.execute(
        "INSERT INTO orders VALUES "
        "(1, 'urgent', 10), "
        "(2, NULL, 5), "
        "(3, 'normal', NULL), "
        "(4, NULL, NULL)"
    )
    return conn


def _make_attrs():
    id_col = Column("id", "INTEGER", "orders")
    notes_col = Column("notes", "VARCHAR", "orders")
    qty_col = Column("quantity", "INTEGER", "orders")
    table = Table("orders", [id_col, notes_col, qty_col])
    id_attr = IntegerAttribute("Id", "id", "INTEGER", "orders")
    notes_attr = StringAttribute("Notes", "notes", "VARCHAR", "orders")
    qty_attr = IntegerAttribute("Quantity", "quantity", "INTEGER", "orders")
    return table, id_attr, notes_attr, qty_attr


class TestIsNoneE2E:

    def test_is_none_filters_null_rows(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, notes_attr.is_none())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert sorted(ids) == [2, 4]

    def test_is_not_none_filters_non_null_rows(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, notes_attr.is_not_none())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert sorted(ids) == [1, 3]

    def test_is_none_integer_column(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, qty_attr.is_none())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert sorted(ids) == [3, 4]

    def test_is_not_none_integer_column(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, qty_attr.is_not_none())
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert sorted(ids) == [1, 2]

    def test_is_none_combined_with_equality(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, notes_attr.is_none().and_op(qty_attr.is_none()))
        rows = con.sql(sql).fetchall()
        ids = [r[0] for r in rows]
        assert ids == [4]

    def test_is_not_none_returns_correct_values(self, con):
        table, id_attr, notes_attr, qty_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr, notes_attr], table, notes_attr.is_not_none())
        rows = con.sql(sql).fetchall()
        assert_array_equal(
            sorted(rows),
            sorted([(1, "urgent"), (3, "normal")]),
        )
