"""Lowering a tree of relations: a step that comes next in a SELECT's order is said by it, and one that comes before is a SELECT over it."""
import pytest

from datafinder import IntegerAttribute, StringAttribute, count_all
from datafinder.relation_planner import alias_of, attributes_of, parts_of
from datafinder.runner import FinderResult
from model.relation_tree import Read
from model.relational import NoOperation, Table, TableAlias

_TABLE = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])
_ID = IntegerAttribute("Id", "ID", "INT", _TABLE)
_NAME = StringAttribute("Name", "NAME", "VARCHAR", _TABLE)


def _rows(*columns) -> FinderResult:
    return FinderResult(None, None, list(columns) or [_ID, _NAME], _TABLE, NoOperation())


def _selects(result: FinderResult) -> int:
    return result.to_sql().count("SELECT")


def test_a_result_that_is_only_read_is_the_tree_of_one_read():
    assert isinstance(_rows()._node, Read)
    assert parts_of(_rows()._node).stage == 0


@pytest.mark.parametrize("steps,selects,ends_with", [
    (lambda r: r.order_by(_ID.ascending()), 1, "ORDER BY t0.ID ASC"),
    (lambda r: r.order_by(_ID.ascending()).limit(3), 1, "ORDER BY t0.ID ASC LIMIT 3"),
    (lambda r: r.order_by(_ID.ascending()).offset(1).limit(3), 1, "ORDER BY t0.ID ASC LIMIT 3 OFFSET 1"),
    (lambda r: r.offset(1).limit(3), 1, "LIMIT 3 OFFSET 1"),
    (lambda r: r.limit(5).limit(2), 1, "LIMIT 2"),
    (lambda r: r.limit(2).limit(5), 1, "LIMIT 2"),
    (lambda r: r.offset(2).offset(3), 1, "OFFSET 5"),
    (lambda r: r.distinct().order_by(_ID.ascending()), 1, "ORDER BY t0.ID ASC"),
])
def test_a_step_that_comes_next_is_said_by_the_select_before_it(steps, selects, ends_with):
    result = steps(_rows())
    assert _selects(result) == selects and result.to_sql().endswith(ends_with)


@pytest.mark.parametrize("steps", [
    lambda r: r.limit(3).offset(1),
    lambda r: r.limit(3).order_by(_ID.ascending()),
    lambda r: r.offset(1).order_by(_ID.ascending()),
    lambda r: r.order_by(_ID.ascending()).distinct(),
    lambda r: r.limit(3).distinct(),
    lambda r: r.offset(1).distinct(),
])
def test_a_step_that_comes_before_the_last_one_said_is_a_select_over_it(steps):
    assert _selects(steps(_rows())) == 2


def test_taking_then_sorting_orders_the_rows_taken_and_sorting_then_taking_takes_the_first_of_the_order():
    taken_then_sorted = _rows().limit(3).order_by(_ID.descending()).to_sql()
    sorted_then_taken = _rows().order_by(_ID.descending()).limit(3).to_sql()
    assert "LIMIT 3) AS" in taken_then_sorted and taken_then_sorted.endswith("ORDER BY t0.id DESC")
    assert sorted_then_taken.count("SELECT") == 1 and sorted_then_taken.endswith("ORDER BY t0.ID DESC LIMIT 3")


def test_a_sort_after_a_limit_is_of_the_columns_the_result_shows():
    with pytest.raises(ValueError, match="ordered by the columns of the result"):
        _rows(_ID).limit(3).order_by(_NAME.ascending())
    ordered = _rows(_ID, _NAME).limit(3).order_by(_NAME.ascending())
    assert ordered.to_sql().endswith("ORDER BY t0.name ASC")


def test_a_node_is_the_same_select_and_the_same_name_each_time():
    result = _rows().limit(3)
    assert parts_of(result._node) is parts_of(result._node)
    assert alias_of(result._node) is alias_of(result._node)
    assert result.to_sql() == result.to_sql()


def test_a_step_leaves_the_node_it_is_written_after_as_it_was():
    rows = _rows()
    rows.order_by(_ID.ascending()).limit(2)
    assert rows.to_sql() == "SELECT t0.ID AS \"Id\",t0.NAME AS \"Name\" FROM accounts AS t0"
    assert rows.limit(1).to_sql().endswith("LIMIT 1") and rows.limit(2).to_sql().endswith("LIMIT 2")


def test_the_columns_a_person_reads_belong_to_the_alias_of_the_node():
    rows = _rows()
    attributes = attributes_of(rows._node)
    assert list(attributes) == ["id", "name"]
    assert attributes["id"].column().owner is alias_of(rows._node) and isinstance(alias_of(rows._node), TableAlias)


def test_a_sort_by_a_column_of_the_node_before_reads_what_that_node_shows():
    rows = _rows()
    ordered = rows.order_by(rows["id"].descending())
    assert _selects(ordered) == 1 and ordered.to_sql().endswith("ORDER BY t0.ID DESC")


def test_a_result_read_twice_in_a_join_is_two_relations():
    left = _rows(_ID.alias("l_id"))
    right = _rows(_ID.alias("r_id"))
    joined = left.join(right, lambda a, b: a.l_id == b.r_id, how="inner")
    sql = joined.to_sql()
    assert sql.count("accounts AS") == 2 and "t1" in sql and "t2" in sql


def test_group_by_and_having_are_said_of_the_rows_a_finder_reads_and_so_come_first():
    grouped = _rows(_NAME, count_all()).group_by(_NAME).having(count_all() > 1).limit(2)
    assert grouped.to_sql().endswith("GROUP BY t0.NAME HAVING COUNT(*) > 1 LIMIT 2")
    with pytest.raises(ValueError, match="group_by is said of the rows a finder reads"):
        _rows(_NAME, count_all()).limit(2).group_by(_NAME)
    with pytest.raises(ValueError, match="having is said of the rows a finder reads"):
        _rows(_NAME, count_all()).group_by(_NAME).order_by(_NAME.ascending()).having(count_all() > 1)
