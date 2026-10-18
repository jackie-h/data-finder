"""FinderResult.to_view: a find_all query with no dates becomes a view, and what a view cannot hold is rejected."""
import datetime

import pytest

from datafinder import FinderResult, ViewDefinition, ViewDefinitionError, ViewResult
from datafinder.typed_attributes import StringAttribute, FloatAttribute
from model.milestoning import MilestonedTable, ProcessingTemporalColumns, SingleBusinessDateColumn
from model.relational import Column, Table, NoOperation, JoinOperation, JoinTreeNodeOperation


def _processing(name):
    return MilestonedTable(name, [], ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")))


def _symbol():
    return StringAttribute("Symbol", "sym", "VARCHAR", "vt.trades")


def _price():
    return FloatAttribute("Price", "price", "DOUBLE", "vt.trades")


def _account_name(child=None):
    child = child if child is not None else _processing("vt.account")
    join = JoinOperation("Account", child, Column("account_id", "INT", "vt.trades"), Column("id", "INT", child.name))
    return StringAttribute("Account Name", "name", "VARCHAR", child.name, JoinTreeNodeOperation(join))


def _query(columns, op=None, table=None) -> FinderResult:
    """What find_all(None, None, columns, op) returns."""
    return FinderResult(None, None, columns, table or _processing("vt.trades"), op or NoOperation())


def test_columns_are_named_after_their_display_names_in_snake_case():
    view = _query([_symbol(), _account_name()]).to_view("v", "reporting")
    assert view.column_names() == ["symbol", "account_name", "in_z", "out_z"]


def test_the_view_renders_ddl_and_wraps_itself_in_a_result():
    view = _query([_symbol()]).to_view("v", "reporting")
    assert isinstance(view, ViewDefinition)
    assert view.to_sql().startswith("CREATE OR REPLACE VIEW reporting.v AS SELECT ")
    assert "`symbol`" in view.to_sql(dialect="databricks")
    assert isinstance(view.result(), ViewResult)


def test_the_filter_becomes_the_views_where_clause():
    sql = _query([_symbol()], _symbol().eq("AAPL").and_op(_price() > 10.0)).to_view("v", "reporting").to_sql()
    assert "WHERE t0.sym = 'AAPL' AND t0.price > 10.0" in sql


def test_a_filter_can_read_through_a_join_that_nothing_projects():
    sql = _query([_symbol()], _account_name().eq("Acme")).to_view("v", "reporting").to_sql()
    assert "LEFT OUTER JOIN vt.account" in sql


def test_aggregates_and_group_by_are_carried_across_and_group_by_the_validity_columns_too():
    name = _account_name()  # one attribute, as a finder hands out: its join is shared by the select list and the group by
    view = _query([name, _price().sum()]).group_by(name).to_view("v", "reporting")
    assert view.column_names() == ["account_name", "sum_price", "in_z", "out_z"]
    assert "GROUP BY t1.name, t0.in_z, t0.out_z" in view.to_sql()


def test_window_functions_keep_every_row_and_use_their_display_names():
    view = _query([_symbol(), _price().rank(method='first', order_by=_price().ascending())]).to_view("v", "reporting")
    assert view.column_names() == ["symbol", "row_number", "in_z", "out_z"]
    assert view.group_by == []
    assert "ROW_NUMBER() OVER (ORDER BY t0.price ASC)" in view.to_sql()


@pytest.mark.parametrize("business_date, processing, business_date_to", [
    (datetime.date(2024, 1, 1), None, None),
    (None, datetime.datetime(2024, 1, 1), None),
    (None, None, datetime.date(2024, 1, 1)),
])
def test_a_query_with_dates_cannot_become_a_view(business_date, processing, business_date_to):
    query = FinderResult(business_date, processing, [_symbol()], _processing("vt.trades"), NoOperation(),
                         business_date_to=business_date_to)
    with pytest.raises(ViewDefinitionError, match="no dates"):
        query.to_view("v", "reporting")


def test_a_view_has_no_order_by_or_limit():
    with pytest.raises(ViewDefinitionError, match="ORDER BY or row limit"):
        _query([_symbol()]).order_by(_symbol().ascending()).to_view("v", "reporting")
    with pytest.raises(ViewDefinitionError, match="ORDER BY or row limit"):
        _query([_symbol()]).limit(5).to_view("v", "reporting")


def test_a_projection_is_required():
    with pytest.raises(ViewDefinitionError, match="at least one"):
        _query([]).to_view("v", "reporting")


def test_two_columns_with_the_same_name_are_rejected():
    with pytest.raises(ViewDefinitionError, match="same name"):
        _query([_symbol(), _symbol()]).to_view("v", "reporting")


@pytest.mark.parametrize("name, schema", [("v; DROP TABLE x", "reporting"), ("v", "bad schema"), ("1v", "reporting")])
def test_names_must_be_plain_identifiers(name, schema):
    with pytest.raises(ViewDefinitionError, match="identifier"):
        _query([_symbol()]).to_view(name, schema)


def test_a_join_that_cannot_be_anchored_is_rejected_when_the_view_is_defined():
    business_dated = MilestonedTable("vt.rate", [], SingleBusinessDateColumn(Column("rate_date", "DATE")))
    with pytest.raises(ViewDefinitionError, match="no business date to anchor"):
        _query([_symbol(), _account_name(business_dated)]).to_view("v", "reporting")


def test_a_non_milestoned_root_gets_no_validity_columns():
    plain = Table("vt.plain", [])
    view = _query([StringAttribute("Name", "name", "VARCHAR", "vt.plain")], table=plain).to_view("v", "reporting")
    assert view.column_names() == ["name"]
