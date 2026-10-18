import pytest

from datafinder import FinderResult
from datafinder.typed_attributes import StringAttribute
from datafinder_liquibase import to_liquibase_changeset, to_liquibase_changelog, write_changelog, \
    to_liquibase_master_changelog
from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Column


def _view(name: str = "trades_v", schema: str = "reporting", symbol: str = "AAPL"):
    table = MilestonedTable("vt.trades", [], ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"),
                                                                       Column("out_z", "TIMESTAMP")))
    sym = StringAttribute("Symbol", "sym", "VARCHAR", "vt.trades")
    return FinderResult(None, None, [sym], table, sym.eq(symbol)).to_view(name, schema)


def test_changeset_is_formatted_sql_with_rollback():
    assert to_liquibase_changeset(_view(), author="jackie", dialect="databricks") == (
        "--changeset jackie:reporting.trades_v runOnChange:true splitStatements:false\n"
        "CREATE OR REPLACE VIEW reporting.trades_v AS SELECT t0.sym AS `symbol`, t0.in_z AS `in_z`, "
        "t0.out_z AS `out_z` FROM vt.trades AS t0 WHERE t0.sym = 'AAPL'\n"
        "--rollback DROP VIEW IF EXISTS reporting.trades_v\n")


def test_changeset_id_override():
    assert "--changeset jackie:trades-1 " in to_liquibase_changeset(_view(), "jackie", changeset_id="trades-1")


def test_changelog_has_header_and_one_changeset_per_view():
    changelog = to_liquibase_changelog([_view("a"), _view("b")], author="jackie", dialect="databricks")
    assert changelog.startswith("--liquibase formatted sql\n\n--changeset jackie:reporting.a ")
    assert changelog.count("--changeset ") == 2
    assert changelog.count("--rollback ") == 2


def test_duplicate_views_rejected():
    with pytest.raises(ValueError, match="more than once"):
        to_liquibase_changelog([_view("a"), _view("A")], author="jackie")


@pytest.mark.parametrize("author", ["", "has space", "a:b", "x\n--changeset evil:1"])
def test_author_must_be_a_plain_token(author):
    with pytest.raises(ValueError, match="Changeset author"):
        to_liquibase_changeset(_view(), author=author)


def test_write_changelog_and_master(tmp_path):
    path = write_changelog([_view()], tmp_path / "db" / "views.sql", author="jackie", dialect="databricks")
    assert path.read_text().startswith("--liquibase formatted sql")
    assert to_liquibase_master_changelog(["views.sql"]) == (
        "databaseChangeLog:\n  - include:\n      file: views.sql\n      relativeToChangelogFile: true\n")
