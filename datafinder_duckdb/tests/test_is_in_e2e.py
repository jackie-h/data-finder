import duckdb
import pytest

from datafinder import to_sql, StringAttribute, IntegerAttribute
from model.relational import Table, Column


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE people (id INTEGER, first_name VARCHAR)")
    conn.execute("INSERT INTO people VALUES (1, 'John'), (2, 'Peter'), (3, 'Mary'), (4, NULL)")
    return conn


def _make_attrs():
    table = Table("people", [Column("id", "INTEGER", "people"), Column("first_name", "VARCHAR", "people")])
    id_attr = IntegerAttribute("Id", "id", "INTEGER", "people")
    name_attr = StringAttribute("First Name", "first_name", "VARCHAR", "people")
    return table, id_attr, name_attr


class TestIsInE2E:

    def test_is_in_string(self, con):
        table, id_attr, name_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, name_attr.is_in(["John", "Peter"]))
        assert "first_name IN ('John', 'Peter')" in sql
        assert sorted(r[0] for r in con.sql(sql).fetchall()) == [1, 2]

    def test_is_not_in_nullable_column_includes_null(self, con):
        table, id_attr, name_attr = _make_attrs()
        sql = to_sql(None, None, [id_attr], table, name_attr.is_not_in(["John", "Peter"]))
        assert "first_name NOT IN ('John', 'Peter') OR" in sql
        assert sorted(r[0] for r in con.sql(sql).fetchall()) == [3, 4]

    def test_is_not_in_not_null_column_has_no_null_check(self, con):
        table, id_attr, _ = _make_attrs()
        name_attr = StringAttribute("First Name", "first_name", "VARCHAR", "people", nullable=False)
        sql = to_sql(None, None, [id_attr], table, name_attr.is_not_in(["John", "Peter"]))
        assert "IS NULL" not in sql
        # the NULL row is excluded, which is why a nullable column has to match it explicitly
        assert sorted(r[0] for r in con.sql(sql).fetchall()) == [3]

    def test_is_in_integer(self, con):
        table, id_attr, name_attr = _make_attrs()
        sql = to_sql(None, None, [name_attr], table, id_attr.is_in([2, 3]))
        assert "id IN (2, 3)" in sql
        assert sorted(r[0] for r in con.sql(sql).fetchall()) == ["Mary", "Peter"]

    def test_is_in_combines_with_other_filters(self, con):
        table, id_attr, name_attr = _make_attrs()
        op = name_attr.is_in(["John", "Peter"]).and_(id_attr.is_not_in([1]))
        rows = con.sql(to_sql(None, None, [id_attr], table, op)).fetchall()
        assert [r[0] for r in rows] == [2]

    def test_empty_list_is_rejected(self):
        _, _, name_attr = _make_attrs()
        with pytest.raises(ValueError):
            name_attr.is_in([])
        with pytest.raises(ValueError):
            name_attr.is_not_in([])
