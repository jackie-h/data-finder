"""contains / starts_with / ends_with match the value literally: %, _ and the escape character are not wildcards."""
import duckdb
import pytest

from datafinder import to_sql, StringAttribute, IntegerAttribute
from model.relational import Table, Column


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE notes (id INTEGER, text VARCHAR)")
    conn.execute(
        "INSERT INTO notes VALUES "
        "(1, '50% off'), (2, '50 percent'), (3, 'a_b'), (4, 'axb'), (5, 'wow!'), (6, 'wow'), "
        "(7, '100%'), (8, 'x_'), (9, 'plain')"
    )
    return conn


def _ids(con, op):
    table = Table("notes", [Column("id", "INTEGER", "notes"), Column("text", "VARCHAR", "notes")])
    id_attr = IntegerAttribute("Id", "id", "INTEGER", "notes")
    return sorted(r[0] for r in con.sql(to_sql(None, None, [id_attr], table, op)).fetchall())


def _text():
    return StringAttribute("Text", "text", "VARCHAR", "notes")


class TestContains:

    def test_a_percent_matches_only_a_percent(self, con):
        assert _ids(con, _text().contains("50%")) == [1]

    def test_an_underscore_matches_only_an_underscore(self, con):
        assert _ids(con, _text().contains("a_b")) == [3]

    def test_the_escape_character_matches_itself(self, con):
        assert _ids(con, _text().contains("!")) == [5]

    def test_a_value_with_no_wildcards_is_unchanged(self, con):
        assert _ids(con, _text().contains("plain")) == [9]


class TestStartsWithAndEndsWith:

    def test_starts_with_a_percent(self, con):
        assert _ids(con, _text().starts_with("50%")) == [1]

    def test_ends_with_a_percent(self, con):
        assert _ids(con, _text().ends_with("%")) == [7]

    def test_ends_with_an_underscore(self, con):
        assert _ids(con, _text().ends_with("_")) == [8]

    def test_starts_with_ordinary_text(self, con):
        assert _ids(con, _text().starts_with("wow")) == [5, 6]


class TestTheSql:

    def test_the_escape_is_named_only_where_it_is_needed(self):
        table = Table("notes", [Column("text", "VARCHAR", "notes")])
        text = _text()
        assert to_sql(None, None, [text], table, text.contains("a_b")).endswith("LIKE '%a!_b%' ESCAPE '!'")
        assert to_sql(None, None, [text], table, text.contains("ab")).endswith("LIKE '%ab%'")
