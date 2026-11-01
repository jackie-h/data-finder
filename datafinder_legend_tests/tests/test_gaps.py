"""A known gap must set aside only the difference it names."""
from datafinder_legend_tests.gaps import gap_is_only
from datafinder_legend_tests.legend import (
    CONCAT_FUNCTION, DATETIME_MILESTONING_DATE, EXISTS_SUB_SELECT_ORDER, ISOLATED_MILESTONED_JOIN, LEFT_JOIN_ORDER,
    NOT_EQUAL_NULL, PROCESSING_SNAPSHOT_DATETIME, TDS_FILTER_OPTIONAL_PROPERTY,
)

def test_a_null_guard_is_covered_only_by_the_optional_property_gap():
    actual = "SELECT t0.age FROM p AS t0 WHERE t0.age > 30"
    expected = "SELECT t0.age FROM p AS t0 WHERE t0.age IS NOT NULL AND t0.age > 30"
    assert gap_is_only(TDS_FILTER_OPTIONAL_PROPERTY, actual, expected)
    assert not gap_is_only(CONCAT_FUNCTION, actual, expected)
    assert not gap_is_only(TDS_FILTER_OPTIONAL_PROPERTY, "SELECT t0.age FROM p AS t0 WHERE t0.age > 31", expected)


def test_a_null_alternative_is_covered_only_by_the_not_equal_gap():
    actual = "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x'"
    expected = "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x' OR t0.a IS NULL"
    assert gap_is_only(NOT_EQUAL_NULL, actual, expected)
    assert not gap_is_only(CONCAT_FUNCTION, actual, expected)
    assert not gap_is_only(NOT_EQUAL_NULL, actual, "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x' OR t0.b IS NULL")


def test_a_datetime_milestoning_date_is_the_date_of_the_datetime():
    legend = ("select \"root\".id as \"id\" from p as \"root\" "
              "where \"root\".snapshotDate = cast(truncate('2015-08-26 02:00:00') as date)")
    assert gap_is_only(DATETIME_MILESTONING_DATE, "SELECT t0.id FROM p AS t0 WHERE t0.snapshotDate = '2015-08-26'", legend)
    assert not gap_is_only(DATETIME_MILESTONING_DATE, "SELECT t0.id FROM p AS t0 WHERE t0.snapshotDate = '2015-08-27'", legend)


def test_a_processing_snapshot_datetime_is_kept_whole():
    legend = ("select \"root\".id as \"id\" from p as \"root\" "
              "where \"root\".snapshotDate = cast(truncate('2015-08-26 02:00:00') as date)")
    assert gap_is_only(PROCESSING_SNAPSHOT_DATETIME, "SELECT t0.id FROM p AS t0 WHERE t0.snapshotDate = '2015-08-26 02:00:00'", legend)
    assert not gap_is_only(PROCESSING_SNAPSHOT_DATETIME, "SELECT t0.id FROM p AS t0 WHERE t0.snapshotDate = '2015-08-26'", legend)


_EXISTS_LEGEND = (
    'select "root".id as "orderId", "producttable_2".name as "productName" from OrderTable as "root" '
    'left outer join (select distinct "producttable_1".id from ProductTable as "producttable_1" '
    "where \"producttable_1\".from_z <= '2015-10-15' and \"producttable_1\".thru_z > '2015-10-15' and \"producttable_1\".id = 2) "
    'as "producttable_0" on ("root".prodFk = "producttable_0".id) '
    'left outer join ProductTable as "producttable_2" on ("root".prodFk = "producttable_2".id) '
    'where "producttable_0".id is not null'
)
_EXISTS_OURS = (
    'SELECT t0.id AS "Id",t1.name AS "Name" FROM main.OrderTable AS t0 '
    'LEFT OUTER JOIN main.ProductTable AS t1 ON (t1.id = t0.prodFk) '
    "LEFT OUTER JOIN (SELECT DISTINCT t3.id AS \"id\" FROM main.ProductTable AS t3 WHERE t3.id = 2 AND t3.from_z <= '2015-10-15' "
    "AND t3.thru_z > '2015-10-15') AS t2 ON (t2.id = t0.prodFk) WHERE t2.id IS NOT NULL"
)


def test_an_exists_sub_select_written_in_another_order_is_covered_by_its_gap():
    assert gap_is_only(EXISTS_SUB_SELECT_ORDER, _EXISTS_OURS, _EXISTS_LEGEND)
    assert not gap_is_only(CONCAT_FUNCTION, _EXISTS_OURS, _EXISTS_LEGEND)
    assert not gap_is_only(ISOLATED_MILESTONED_JOIN, _EXISTS_OURS, _EXISTS_LEGEND)


def test_an_exists_gap_does_not_cover_a_different_filter_value():
    assert not gap_is_only(EXISTS_SUB_SELECT_ORDER, _EXISTS_OURS.replace("t3.id = 2", "t3.id = 3"), _EXISTS_LEGEND)


def test_an_exists_gap_does_not_cover_a_different_milestoning_date():
    assert not gap_is_only(EXISTS_SUB_SELECT_ORDER, _EXISTS_OURS.replace("t3.from_z <= '2015-10-15'", "t3.from_z <= '2015-10-16'"), _EXISTS_LEGEND)


def test_independent_left_joins_in_another_order_are_covered_but_a_changed_condition_is_not():
    legend = ("select 1 from a as \"root\" left outer join b as \"b_0\" on (\"root\".x = \"b_0\".x) "
              "left outer join c as \"c_0\" on (\"root\".y = \"c_0\".y)")
    swapped = "SELECT 1 FROM a AS t0 LEFT OUTER JOIN c AS t1 ON (t0.y = t1.y) LEFT OUTER JOIN b AS t2 ON (t0.x = t2.x)"
    assert gap_is_only(LEFT_JOIN_ORDER, swapped, legend)
    assert not gap_is_only(LEFT_JOIN_ORDER, swapped.replace("t0.y = t1.y", "t0.z = t1.y"), legend)
