"""Join checks and rendering for views: anchoring to the root row, and the DDL."""
import datetime
import pytest

from datafinder.sql_generator import build_as_of_join_condition, check_view_joins, view_sql_to_string
from model.milestoning import MilestonedTable, ProcessingTemporalColumns, SingleBusinessDateColumn, \
    BusinessDateAndProcessingTemporalColumns, BiTemporalColumns
from model.relational import Column, Table, Alias, ColumnWithJoin, ComparisonOperation, JoinOperation, \
    JoinTreeNodeOperation
from model.view import View, ViewDefinitionError


def _processing(name):
    return MilestonedTable(name, [], ProcessingTemporalColumns(Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")))


def _business(name):
    return MilestonedTable(name, [], SingleBusinessDateColumn(Column("bdate", "DATE")))


def _bitemporal(name):
    return MilestonedTable(name, [], BiTemporalColumns(Column("d_from", "DATE"), Column("d_to", "DATE"),
                                                       Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")))


def _node(child, name="Child"):
    join = JoinOperation(name, child, Column("fk", "INT", "root"), Column("id", "INT", child.name))
    return JoinTreeNodeOperation(join, None)


def _alias(name, table="root", node=None):
    return Alias(ColumnWithJoin(Column(name, "VARCHAR", table), node), name)


def test_rendering_pins_the_joined_table_to_the_root_row():
    root, child = _processing("root"), _processing("child")
    node = _node(child)
    view = View("v", "s", root, [_alias("x"), _alias("y", "child", node)])
    sql = view_sql_to_string(view)
    assert node.join.filter is None
    assert id(node) in view.join_filters
    assert "t1.in_z <= t0.in_z" in sql


def test_processing_child_needs_a_processing_root():
    view = View("v", "s", _business("root"), [_alias("y", "child", _node(_processing("child")))])
    with pytest.raises(ViewDefinitionError, match="no processing time"):
        check_view_joins(view)


@pytest.mark.parametrize("child", [_business, _bitemporal])
def test_business_child_needs_a_business_root(child):
    view = View("v", "s", _processing("root"), [_alias("y", "child", _node(child("child")))])
    with pytest.raises(ViewDefinitionError, match="no business date"):
        check_view_joins(view)


def test_bitemporal_root_anchors_on_its_business_date_from():
    condition = build_as_of_join_condition(_bitemporal("root"), _node(_business("child")))
    assert isinstance(condition, ComparisonOperation)
    assert isinstance(condition.right, Column) and condition.right.name == "d_from"


def test_business_date_and_processing_child_gets_both_axes():
    def both(name):
        return MilestonedTable(name, [], BusinessDateAndProcessingTemporalColumns(
            Column("bdate", "DATE"), Column("in_z", "TIMESTAMP"), Column("out_z", "TIMESTAMP")))
    assert build_as_of_join_condition(both("root"), _node(both("child"))) is not None


def test_non_milestoned_child_needs_no_condition():
    assert build_as_of_join_condition(_processing("root"), _node(Table("child", []))) is None


def test_an_ancestor_that_cannot_be_anchored_is_rejected_too():
    parent = _node(_business("mid"), name="Mid")
    leaf_join = JoinOperation("Leaf", Table("leaf", []), Column("fk", "INT", "mid"), Column("id", "INT", "leaf"))
    view = View("v", "s", _processing("root"), [_alias("y", "leaf", JoinTreeNodeOperation(leaf_join, parent))])
    with pytest.raises(ViewDefinitionError, match="'Mid'"):
        check_view_joins(view)


def test_joins_read_only_through_group_by_are_still_checked():
    mid = _node(_business("child"))
    view = View("v", "s", _processing("root"), [_alias("x")],
                group_by=[ColumnWithJoin(Column("y", "VARCHAR", "child"), mid)])
    with pytest.raises(ViewDefinitionError, match="no business date"):
        check_view_joins(view)


def test_rendering_refuses_a_view_whose_joins_fail_the_check():
    view = View("v", "s", _processing("root"), [_alias("y", "child", _node(_business("child")))])
    with pytest.raises(ViewDefinitionError, match="no business date"):
        view_sql_to_string(view)


def test_building_one_query_does_not_change_another_on_a_shared_join():
    from datafinder import Attribute, StringAttribute
    from datafinder.sql_generator import build_query_operation, select_sql_to_string
    from model.relational import NoOperation
    root, child = _processing("root"), _processing("child")
    node = _node(child)
    columns: list[Attribute] = [StringAttribute("Y", "y", "VARCHAR", "child", node)]

    def build(year):
        return build_query_operation(None, datetime.datetime(year, 6, 1, 12), columns, root, NoOperation())

    first = build(2021)
    second = build(2023)
    sql = select_sql_to_string(first, validate_sqlglot=False)
    assert "2021" in sql and "2023" not in sql
    assert node.join.filter is None
    assert "2023" in select_sql_to_string(second, validate_sqlglot=False)
