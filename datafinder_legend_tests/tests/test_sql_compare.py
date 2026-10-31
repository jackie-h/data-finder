"""The comparison must ignore cosmetic differences and still catch real ones."""
import pytest

from datafinder_examples_tests.sql_compare import sql_equivalent

LEGEND = (
    'select "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "legalName" from personTable as "root" '
    'left outer join firmTable as "firmtable_0" on ("root".FIRMID = "firmtable_0".ID) '
    "where \"firmtable_0\".LEGALNAME = 'Firm X'"
)
OURS = (
    'SELECT t0.FIRSTNAME AS "FirstName",t1.LEGALNAME AS "Firm LegalName" FROM default.personTable AS t0 '
    "LEFT OUTER JOIN default.firmTable AS t1 ON t0.FIRMID = t1.ID WHERE t1.LEGALNAME = 'Firm X'"
)


def test_cosmetic_differences_are_ignored():
    assert sql_equivalent(OURS, LEGEND)


@pytest.mark.parametrize("changed", [
    OURS.replace("t0.FIRSTNAME", "t0.LASTNAME"),
    OURS.replace("t0.FIRMID = t1.ID", "t0.ID = t1.ID"),
    OURS.replace("LEFT OUTER JOIN", "INNER JOIN"),
    OURS.replace("'Firm X'", "'Firm Y'"),
    OURS.replace(" WHERE t1.LEGALNAME = 'Firm X'", ""),
    OURS.replace("SELECT t0.FIRSTNAME AS \"FirstName\",t1.LEGALNAME", "SELECT t1.LEGALNAME,t0.FIRSTNAME"),
    OURS.replace("default.firmTable", "default.otherTable"),
])
def test_real_differences_are_caught(changed):
    assert not sql_equivalent(changed, LEGEND)


@pytest.mark.parametrize("actual,expected", [
    # operand order
    ("SELECT a FROM t WHERE x = 1", "SELECT a FROM t WHERE 1 = x"),
    # AND order
    ("SELECT a FROM t WHERE x = 1 AND y = 2", "SELECT a FROM t WHERE y = 2 AND x = 1"),
    ("SELECT a FROM t WHERE (x = 1 AND y = 2) AND z = 3", "SELECT a FROM t WHERE x = 1 AND (z = 3 AND y = 2)"),
    # a timestamp is not the date it falls on
    ("SELECT a FROM t WHERE d <= '2017-06-11 00:00:00'", "SELECT a FROM t WHERE d <= '2017-06-11'"),
    # AND is not OR
    ("SELECT a FROM t WHERE x = 1 AND y = 2", "SELECT a FROM t WHERE x = 1 OR y = 2"),
    # grouping
    ("SELECT a FROM t WHERE (x = 1 OR y = 2) AND z = 3", "SELECT a FROM t WHERE x = 1 OR (y = 2 AND z = 3)"),
    # tables must pair up with the same table
    ("SELECT t0.a FROM t AS t0 JOIN u AS t1 ON t0.k = t1.k", "SELECT t0.a FROM t AS t0 JOIN v AS t1 ON t0.k = t1.k"),
    # which of two aliases a column comes from
    ("SELECT t1.a FROM t AS t0 JOIN t AS t1 ON t0.k = t1.k", "SELECT t0.a FROM t AS t0 JOIN t AS t1 ON t0.k = t1.k"),
])
def test_expression_differences_are_caught(actual, expected):
    assert not sql_equivalent(actual, expected)


def test_table_aliases_are_paired_by_position():
    assert sql_equivalent(
        "SELECT x.a FROM t AS x JOIN u AS y ON x.k = y.k WHERE y.b = 1",
        'SELECT "root".a FROM t AS "root" JOIN u AS "u_0" ON "root".k = "u_0".k WHERE "u_0".b = 1',
    )


def test_parentheses_around_a_single_predicate_are_ignored():
    assert sql_equivalent("SELECT a FROM t JOIN u ON (t.k = u.k)", "SELECT a FROM t JOIN u ON t.k = u.k")


def test_parentheses_around_a_whole_on_or_where_condition_are_ignored():
    assert sql_equivalent("SELECT a FROM t JOIN u ON (t.k = u.k AND u.b = 1) WHERE (t.x = 1 AND t.y = 2)",
                          "SELECT a FROM t JOIN u ON t.k = u.k AND u.b = 1 WHERE t.x = 1 AND t.y = 2")


@pytest.mark.parametrize("actual,expected", [
    ("SELECT a FROM t WHERE (x = 1 AND y = 2) AND z = 3", "SELECT a FROM t WHERE x = 1 AND (y = 2 AND z = 3)"),
    ("SELECT a FROM t WHERE x = 1 AND (y = 2 AND z = 3)", "SELECT a FROM t WHERE x = 1 AND y = 2 AND z = 3"),
    ("SELECT a FROM t JOIN u ON (t.k = u.k AND (u.b = 1 AND u.c = 2))", "SELECT a FROM t JOIN u ON (t.k = u.k AND u.b = 1 AND u.c = 2)"),
    ("SELECT a FROM t WHERE (x = 1 OR y = 2) OR z = 3", "SELECT a FROM t WHERE x = 1 OR (y = 2 OR z = 3)"),
    ("SELECT a FROM t WHERE x = 1 OR (y = 2 AND z = 3)", "SELECT a FROM t WHERE x = 1 OR y = 2 AND z = 3"),
    ("SELECT a FROM t WHERE (x = 1 AND y = 2) OR z = 3", "SELECT a FROM t WHERE x = 1 AND y = 2 OR z = 3"),
    ("SELECT a FROM t WHERE (x = 1 OR y = 2) AND (z = 3 AND (w = 4 OR v = 5))",
     "SELECT a FROM t WHERE ((x = 1 OR y = 2) AND z = 3) AND (w = 4 OR v = 5)"),
])
def test_grouping_of_the_same_operator_is_ignored(actual, expected):
    assert sql_equivalent(actual, expected)


def test_associativity_does_not_make_operand_order_irrelevant():
    assert not sql_equivalent("SELECT a FROM t WHERE x = 1 AND (y = 2 AND z = 3)",
                              "SELECT a FROM t WHERE (x = 1 AND z = 3) AND y = 2")


def test_parentheses_that_change_meaning_are_not_ignored():
    assert not sql_equivalent("SELECT a FROM t WHERE (x = 1 OR y = 2) AND z = 3", "SELECT a FROM t WHERE x = 1 OR y = 2 AND z = 3")
    assert not sql_equivalent("SELECT a FROM t WHERE x = 1 AND (y = 2 OR z = 3)", "SELECT a FROM t WHERE x = 1 AND y = 2 OR z = 3")


def test_a_clause_that_names_an_output_alias_is_the_expression_it_stands_for():
    assert sql_equivalent("SELECT t0.AGE AS a FROM t AS t0 ORDER BY t0.AGE ASC", 'SELECT "r".AGE AS "age" FROM t AS "r" ORDER BY "age" ASC')
    assert sql_equivalent("SELECT t0.k AS a, SUM(t0.v) AS s FROM t AS t0 GROUP BY t0.k", 'SELECT "r".k AS "key", SUM("r".v) AS "s" FROM t AS "r" GROUP BY "key"')


def test_ordering_by_a_different_expression_is_still_caught():
    assert not sql_equivalent("SELECT t0.AGE AS a FROM t AS t0 ORDER BY t0.OTHER ASC", 'SELECT "r".AGE AS "age" FROM t AS "r" ORDER BY "age" ASC')


def test_fetch_next_is_a_limit():
    assert sql_equivalent("SELECT a FROM t LIMIT 2 OFFSET 1", "select a from t offset 1 rows fetch next 2 rows only")
    assert not sql_equivalent("SELECT a FROM t LIMIT 3 OFFSET 1", "select a from t offset 1 rows fetch next 2 rows only")


def test_listagg_is_a_string_aggregate():
    assert sql_equivalent("SELECT STRING_AGG(a, '*') FROM t", "select listagg(a, '*') from t")
    assert not sql_equivalent("SELECT STRING_AGG(a, '-') FROM t", "select listagg(a, '*') from t")


def test_in_with_one_value_is_equality():
    assert sql_equivalent("SELECT a FROM t WHERE a IN ('x')", "select a from t where a = 'x'")
    assert not sql_equivalent("SELECT a FROM t WHERE a IN ('x', 'y')", "select a from t where a = 'x'")


@pytest.mark.parametrize("actual,expected", [
    ("SELECT a FROM t JOIN u ON t.k = u.k", "SELECT a FROM t JOIN u ON u.k = t.k"),
    ("SELECT a FROM t LEFT JOIN u ON (t.k = u.k AND u.d = '2015-08-26')", "SELECT a FROM t LEFT JOIN u ON (u.k = t.k AND '2015-08-26' = u.d)"),
    ("SELECT a FROM t JOIN u ON t.k = u.k JOIN v ON u.j = v.j", "SELECT a FROM t JOIN u ON u.k = t.k JOIN v ON v.j = u.j"),
])
def test_the_two_sides_of_an_equality_in_a_join_condition_are_interchangeable(actual, expected):
    assert sql_equivalent(actual, expected)


@pytest.mark.parametrize("actual,expected", [
    # only an equality in a join: the same one in a WHERE is compared as written
    ("SELECT a FROM t JOIN u ON t.k = u.k WHERE t.x = u.y", "SELECT a FROM t JOIN u ON t.k = u.k WHERE u.y = t.x"),
    # only an equality: an ordering comparison is not turned around
    ("SELECT a FROM t JOIN u ON t.k < u.k", "SELECT a FROM t JOIN u ON u.k < t.k"),
    # the join still has to be the same join
    ("SELECT a FROM t JOIN u ON t.k = u.k", "SELECT a FROM t JOIN u ON t.k = u.j"),
    ("SELECT a FROM t JOIN u ON t.k = u.k AND t.j = u.j", "SELECT a FROM t JOIN u ON t.j = u.j AND t.k = u.k"),
])
def test_only_an_equality_in_a_join_condition_is_interchangeable(actual, expected):
    assert not sql_equivalent(actual, expected)


def test_parentheses_around_a_whole_case_when_condition_group_nothing():
    assert sql_equivalent("SELECT CASE WHEN a IS NOT NULL AND a LIKE 'x%' THEN 1 ELSE 0 END FROM t",
                          "SELECT CASE WHEN (a IS NOT NULL AND a LIKE 'x%') THEN 1 ELSE 0 END FROM t")
    # but they do when they are not the whole condition
    assert not sql_equivalent("SELECT CASE WHEN (a = 1 OR b = 2) AND c = 3 THEN 1 ELSE 0 END FROM t",
                              "SELECT CASE WHEN a = 1 OR b = 2 AND c = 3 THEN 1 ELSE 0 END FROM t")
