"""A known gap must set aside only the difference it names."""
from datafinder_legend_tests.gaps import gap_is_only
from datafinder_legend_tests.legend import JOIN_OPERAND_ORDER, NOT_EQUAL_NULL, OPTIONAL_PROPERTY

_ACTUAL = "SELECT t0.a FROM p AS t0 LEFT OUTER JOIN f AS t1 ON (t0.fk = t1.id) WHERE t0.x = 1 ORDER BY t0.a"


def test_swapped_join_operands_are_covered_by_the_join_operand_gap():
    assert gap_is_only(JOIN_OPERAND_ORDER, _ACTUAL, "SELECT t0.a FROM p AS t0 LEFT OUTER JOIN f AS t1 ON (t1.id = t0.fk) WHERE t0.x = 1 ORDER BY t0.a")


def test_another_difference_is_not_hidden_by_the_join_operand_gap():
    assert not gap_is_only(JOIN_OPERAND_ORDER, _ACTUAL, "SELECT t0.a FROM p AS t0 LEFT OUTER JOIN f AS t1 ON (t1.id = t0.fk) WHERE t0.x = 2 ORDER BY t0.a")
    assert not gap_is_only(JOIN_OPERAND_ORDER, _ACTUAL, "SELECT t0.b FROM p AS t0 LEFT OUTER JOIN f AS t1 ON (t1.id = t0.fk) WHERE t0.x = 1 ORDER BY t0.a")


def test_a_different_ordering_is_not_covered_by_the_join_operand_gap():
    expected = "SELECT t0.a FROM p AS t0 LEFT OUTER JOIN f AS t1 ON (t0.fk = t1.id) WHERE t0.x = 1 ORDER BY t0.b, t0.a"
    assert not gap_is_only(JOIN_OPERAND_ORDER, _ACTUAL, expected)


def test_a_null_guard_is_covered_only_by_the_optional_property_gap():
    actual = "SELECT t0.age FROM p AS t0 WHERE t0.age > 30"
    expected = "SELECT t0.age FROM p AS t0 WHERE t0.age IS NOT NULL AND t0.age > 30"
    assert gap_is_only(OPTIONAL_PROPERTY, actual, expected)
    assert not gap_is_only(JOIN_OPERAND_ORDER, actual, expected)
    assert not gap_is_only(OPTIONAL_PROPERTY, "SELECT t0.age FROM p AS t0 WHERE t0.age > 31", expected)


def test_a_null_alternative_is_covered_only_by_the_not_equal_gap():
    actual = "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x'"
    expected = "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x' OR t0.a IS NULL"
    assert gap_is_only(NOT_EQUAL_NULL, actual, expected)
    assert not gap_is_only(JOIN_OPERAND_ORDER, actual, expected)
    assert not gap_is_only(NOT_EQUAL_NULL, actual, "SELECT t0.a FROM p AS t0 WHERE t0.a <> 'x' OR t0.b IS NULL")
