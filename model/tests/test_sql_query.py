"""A query written as PostgreSQL is read into the query model, and what it cannot read is refused with the part it is."""
import pytest

from model.relational import (
    Alias, AggregateOperation, AggregateOperator, CaseOperation, ComparisonOperation, ConcatOperation, JoinKind,
    LogicalOperation, QuerySpecification, Table, TableAlias, UnionOperation,
)
from model.sql_query import SqlQueryError, output_names, parse_query, parse_query_reads

_PERSON = Table("personTable", [("ID", "INT"), ("FIRSTNAME", "VARCHAR"), ("FIRMID", "INT"), ("AGE", "INT")])
_FIRM = Table("firmTable", [("ID", "INT"), ("LEGALNAME", "VARCHAR")])
_TABLES = [_PERSON, _FIRM]


def _parse(sql):
    return parse_query(sql, _TABLES, "Query 'V'")


def test_distinct_over_one_table():
    query = _parse("SELECT DISTINCT FIRMID, FIRSTNAME FROM personTable")
    assert isinstance(query, QuerySpecification) and query.distinct
    assert output_names(query) == ["FIRMID", "FIRSTNAME"]
    assert query.table is _PERSON and parse_query_reads("SELECT DISTINCT FIRMID, FIRSTNAME FROM personTable", _TABLES)[1] == [_PERSON]


def test_a_filter_through_an_inner_join_with_aliases():
    query = _parse("SELECT p.ID, p.AGE, f.LEGALNAME FROM personTable p INNER JOIN firmTable f ON f.ID = p.FIRMID "
                   "WHERE f.LEGALNAME = 'Firm X' AND p.AGE IS NOT NULL")
    assert isinstance(query, QuerySpecification)
    assert output_names(query) == ["ID", "AGE", "LEGALNAME"]
    assert len(query.joins) == 1 and query.joins[0].kind == JoinKind.INNER
    assert isinstance(query.filter, LogicalOperation)
    assert isinstance(query.table, TableAlias) and query.table.relation is _PERSON


def test_group_by_with_an_aggregate_and_having():
    query = _parse("SELECT FIRMID, SUM(AGE) AS total FROM personTable GROUP BY FIRMID HAVING SUM(AGE) > 10")
    assert isinstance(query, QuerySpecification)
    assert output_names(query) == ["FIRMID", "total"]
    total = query.columns[1]
    assert isinstance(total, Alias) and isinstance(total.element, AggregateOperation)
    assert total.element.operator == AggregateOperator.SUM
    assert len(query.group_by) == 1 and isinstance(query.having, ComparisonOperation)


def test_expressions_with_names():
    query = _parse("SELECT ID, 'a' || FIRSTNAME AS tag, CASE WHEN AGE > 30 THEN 'old' ELSE 'young' END AS age_band, "
                   "UPPER(FIRSTNAME) AS shout FROM personTable")
    assert output_names(query) == ["ID", "tag", "age_band", "shout"]
    assert isinstance(query.columns[1].element, ConcatOperation)  # type: ignore[attr-defined]
    assert isinstance(query.columns[2].element, CaseOperation)  # type: ignore[attr-defined]


def test_is_not_null_is_not_is_null():
    """PostgreSQL reads IS NOT NULL as an IS NULL that is negated, which is not the same condition."""
    from model.relational import ComparisonOperator, IsNotNullOperation, IsNullOperation
    assert isinstance(_parse("SELECT ID FROM personTable WHERE AGE IS NOT NULL").filter, IsNotNullOperation)  # type: ignore[attr-defined]
    assert isinstance(_parse("SELECT ID FROM personTable WHERE AGE IS NULL").filter, IsNullOperation)  # type: ignore[attr-defined]
    like = _parse("SELECT ID FROM personTable WHERE FIRSTNAME NOT LIKE 'A%'").filter  # type: ignore[attr-defined]
    assert isinstance(like, ComparisonOperation) and like.operator == ComparisonOperator.NOT_LIKE


def test_a_union_is_a_union_of_its_sides():
    query = _parse("SELECT ID FROM personTable UNION ALL SELECT ID FROM firmTable")
    assert isinstance(query, UnionOperation) and query.all
    assert parse_query_reads("SELECT ID FROM personTable UNION ALL SELECT ID FROM firmTable", _TABLES)[1] == [_PERSON, _FIRM]
    assert not _parse("SELECT ID FROM personTable UNION SELECT ID FROM firmTable").all  # type: ignore[attr-defined]


def test_star_is_every_column():
    assert output_names(_parse("SELECT * FROM firmTable")) == ["ID", "LEGALNAME"]
    assert output_names(_parse("SELECT f.* FROM firmTable f")) == ["ID", "LEGALNAME"]


@pytest.mark.parametrize("sql,message", [
    ("SELEC ID FROM personTable", "cannot be read"),
    ("SELECT ID FROM nowhere", "is not one of the tables there are"),
    ("SELECT NOPE FROM personTable", "no table of the query has a column 'NOPE'"),
    ("SELECT ID FROM personTable p JOIN firmTable f ON f.ID = p.FIRMID", "is in more than one table"),
    ("SELECT p.NOPE FROM personTable p", "has no column 'NOPE'"),
    ("SELECT x.ID FROM personTable p", "is not a table of the query"),
    ("SELECT ID FROM personTable ORDER BY ID", "ORDER is not part of a mapped query"),
    ("SELECT ID FROM personTable LIMIT 1", "LIMIT is not part of a mapped query"),
    ("SELECT ID + 1 FROM personTable", "has no name"),
    ("SELECT ID FROM personTable a, firmTable b", "not part"),
    ("SELECT ID FROM personTable INTERSECT SELECT ID FROM firmTable", "only UNION"),
    ("SELECT ID, ID FROM personTable", "names ['ID'] more than once"),
    ("SELECT SQRT(AGE) AS s FROM personTable", "is not something a view can read yet"),
])
def test_what_cannot_be_read_is_refused_and_says_why(sql, message):
    with pytest.raises(SqlQueryError) as error:
        _parse(sql)
    assert message in str(error.value) or message == "not part"
