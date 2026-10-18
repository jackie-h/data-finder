from typing import Any

import pytest

from model.milestoning import MilestonedTable, ProcessingTemporalColumns
from model.relational import Column, Table, Alias, SelectOperation, QueryExpression, Relation, \
    AggregateOperation, AggregateOperator, NoOperation, ComparisonOperation, ComparisonOperator, \
    IntegerConstantOperation, WindowFunction, WindowFunctionOperation
from model.view import View, ViewDefinitionError


def _processing(name):
    return MilestonedTable(name, [], ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")))


def _alias(name, table="root"):
    return Alias(Column(name, "VARCHAR", table), name)


def _column_names(expressions):
    return [e.name for e in expressions if isinstance(e, Column)]


def _sum_of(name, alias):
    return Alias(AggregateOperation(Column(name, "DOUBLE", "root"), AggregateOperator.SUM), alias)


def test_a_view_is_a_relation_and_shares_its_query_base_with_select():
    view = View("v", "s", _processing("root"), [_alias("x")])
    assert isinstance(view, Relation) and isinstance(view, QueryExpression)
    assert isinstance(SelectOperation([], NoOperation()), QueryExpression)
    assert not isinstance(view, SelectOperation)
    # what a view can't hold: it is stored, not run
    assert view.order_by == [] and view.limit is None


def test_a_view_exposes_the_projection_then_the_root_milestoning_columns():
    view = View("v", "s", _processing("root"), [_alias("x")])
    assert view.qualified_name == "s.v"
    assert view.column_names() == ["x", "in_z", "out_z"]


def test_non_milestoned_root_exposes_only_the_projection():
    assert View("v", "s", Table("root", []), [_alias("x")]).column_names() == ["x"]


def test_a_query_lists_its_expressions_in_render_order():
    display, group, filter_op = _alias("x"), Column("g", "VARCHAR", "root"), NoOperation()
    query = SelectOperation([display], filter_op, order_by=[], group_by=[group])
    assert query.expressions() == [display, group, filter_op]


def test_aggregates_are_allowed_and_group_by_the_root_validity_columns():
    key = Column("k", "VARCHAR", "root")
    view = View("v", "s", _processing("root"), [Alias(key, "k"), _sum_of("x", "total")], group_by=[key])
    assert view.column_names() == ["k", "total", "in_z", "out_z"]
    assert _column_names(view.group_by) == ["k", "in_z", "out_z"]


def test_an_aggregate_without_group_by_still_groups_by_the_validity_columns():
    view = View("v", "s", _processing("root"), [_sum_of("x", "total")])
    assert _column_names(view.group_by) == ["in_z", "out_z"]


def test_ungrouped_view_has_no_group_by():
    assert View("v", "s", _processing("root"), [_alias("x")]).group_by == []


def test_a_non_milestoned_root_adds_nothing_to_the_grouping():
    view = View("v", "s", Table("root", []), [_sum_of("x", "total")])
    assert view.group_by == []


def test_window_functions_are_allowed_and_do_not_group():
    window = Alias(WindowFunctionOperation(None, WindowFunction.ROW_NUMBER), "n")
    view = View("v", "s", _processing("root"), [_alias("x"), window])
    assert view.group_by == []


def test_filter_and_group_by_cannot_contain_aggregates():
    root = _processing("root")
    total = AggregateOperation(Column("x", "DOUBLE", "root"), AggregateOperator.SUM)
    with pytest.raises(ViewDefinitionError, match="filter"):
        View("v", "s", root, [_alias("x")], filter=ComparisonOperation(total, ComparisonOperator.GREATER_THAN,
                                                                       IntegerConstantOperation(1)))
    with pytest.raises(ViewDefinitionError, match="group by"):
        View("v", "s", root, [_alias("x")], group_by=[total])


def test_rejects_empty_projection_unnamed_columns_duplicates_and_bad_identifiers():
    root = _processing("root")
    with pytest.raises(ViewDefinitionError, match="at least one"):
        View("v", "s", root, [])
    unnamed: list[Any] = [Column("x", "INT", "root")]
    with pytest.raises(ViewDefinitionError, match="needs a name"):
        View("v", "s", root, unnamed)
    with pytest.raises(ViewDefinitionError, match="same name"):
        View("v", "s", root, [_alias("x"), _alias("X")])
    with pytest.raises(ViewDefinitionError, match="same name"):
        View("v", "s", root, [_alias("in_z")])
    with pytest.raises(ViewDefinitionError, match="identifier"):
        View("v; DROP", "s", root, [_alias("x")])
    with pytest.raises(ViewDefinitionError, match="identifier"):
        View("v", "bad schema", root, [_alias("x")])
