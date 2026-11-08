"""A union of query expressions, and ``concatenate``, which gives the union of two results as a result."""
import pytest

from datafinder import DoubleAttribute, IntegerAttribute, StringAttribute
from datafinder.runner import FinderResult
from datafinder.sql_generator import build_query_specification
from model.relational import (
    NamedRelation, NoOperation, QueryExpression, QuerySpecification, Relation, Table, UnionOperation)

_TABLE = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])
_ID = IntegerAttribute("Id", "ID", "INT", _TABLE)
_NAME = StringAttribute("Name", "NAME", "VARCHAR", _TABLE)


def _select(columns):
    return build_query_specification(None, None, columns, _TABLE, NoOperation())


def _result(columns, op=None):
    return FinderResult(None, None, columns, _TABLE, op if op is not None else NoOperation())


def test_a_union_is_a_query_expression_and_not_a_single_select():
    union = UnionOperation(_select([_NAME]), _select([_NAME]))
    assert isinstance(union, QueryExpression) and not isinstance(union, QuerySpecification)
    assert union.all is False
    assert UnionOperation(_select([_NAME]), _select([_NAME]), all=True).all is True


def test_the_two_sides_of_a_union_have_the_same_columns():
    with pytest.raises(ValueError, match="same columns"):
        UnionOperation(_select([_NAME]), _select([_NAME, _ID]))


def test_concatenate_is_a_result_over_the_union_of_the_two():
    union = _result([_NAME, _ID], _ID.eq(1)).concatenate(_result([_NAME, _ID], _ID.eq(2)))
    assert isinstance(union, FinderResult)
    assert union.columns() == ["name", "id"]
    assert union.to_sql() == (
        'SELECT t0.name AS "name",t0.id AS "id" FROM ('
        'SELECT t1.NAME AS "name",t1.ID AS "id" FROM accounts AS t1 WHERE t1.ID = 1 '
        'UNION SELECT t2.NAME AS "name",t2.ID AS "id" FROM accounts AS t2 WHERE t2.ID = 2) AS t0')


def test_concatenate_all_keeps_the_duplicate_rows():
    sql = _result([_NAME]).concatenate(_result([_NAME]), all=True).to_sql()
    assert " UNION ALL SELECT" in sql


def test_concatenate_needs_the_same_columns():
    with pytest.raises(ValueError, match="same columns"):
        _result([_NAME]).concatenate(_result([_NAME, _ID]))
    with pytest.raises(ValueError, match="same columns"):
        _result([_NAME]).concatenate(_result([_ID]))


def test_a_concatenated_result_is_ordered_and_limited_like_any_other():
    union = _result([_NAME, _ID]).concatenate(_result([_NAME, _ID]))
    union.order_by(union["name"].ascending(), union["id"].descending()).offset(2).limit(5)
    assert union.to_sql().endswith(") AS t0 ORDER BY t0.name ASC, t0.id DESC LIMIT 5 OFFSET 2")
    with pytest.raises(KeyError, match="No column 'missing'"):
        union["missing"]


def test_concatenate_chains_and_a_side_with_an_order_is_in_brackets():
    first = _result([_NAME]).concatenate(_result([_NAME]), all=True)
    three = first.concatenate(_result([_NAME]), all=True)
    assert three.to_sql().count("UNION ALL") == 2
    ordered = _result([_NAME]).concatenate(_result([_NAME]))
    ordered.order_by(ordered["name"].ascending()).limit(1)
    sql = _result([_NAME]).concatenate(ordered, all=True).to_sql()
    assert "UNION ALL (SELECT" in sql and "LIMIT 1)" in sql


def test_each_select_of_a_union_has_aliases_of_its_own():
    ordered = _result([_NAME]).concatenate(_result([_NAME]))
    ordered.order_by(ordered["name"].ascending()).limit(1)
    sql = _result([_NAME]).concatenate(ordered, all=True).to_sql()
    aliases = [part.split(" ")[0] for part in sql.split(" AS ")[1:]]
    defined = [alias for alias in aliases if alias.startswith("t")]
    assert len(defined) == len(set(defined))


def test_concatenating_does_not_change_the_results_it_is_made_from():
    left, right = _result([_NAME]), _result([_NAME])
    left.concatenate(right)
    assert left.to_sql() == 'SELECT t0.NAME AS "Name" FROM accounts AS t0'
    assert isinstance(left.concatenate(right)._table.relation, UnionOperation)  # type: ignore[attr-defined]


def test_the_columns_of_a_union_are_those_of_its_left_side():
    union = UnionOperation(_select([_NAME, _ID]), _select([_NAME, _ID]))
    assert isinstance(union, Relation) and not isinstance(union, NamedRelation)
    assert list(union.columns) == [_NAME, _ID]


def test_a_result_of_concatenate_reads_from_an_alias_of_the_union():
    from model.relational import TableAlias
    result = _result([_NAME]).concatenate(_result([_NAME]))
    assert isinstance(result._table, TableAlias) and isinstance(result._table.relation, UnionOperation)
    assert result._table.name.startswith("relation")


def test_the_columns_of_a_union_have_the_types_of_what_the_results_show():
    from datafinder import constant
    from datafinder.relation import column_type
    from model.relational import Alias
    price = _ID * 2  # an integer
    ratio = _ID / 2  # a double
    first = _result([_NAME, Alias(price, "doubled"), Alias(ratio, "half"), Alias(_NAME.length(), "size"),
                     Alias(_ID.sum(), "total"), Alias(constant(1.5), "rate"), Alias(_NAME.upper(), "loud")])
    types = {name: column_type(element) for name, element in zip(first.columns(), first._columns)}
    assert types == {"name": "VARCHAR", "doubled": "BIGINT", "half": "DOUBLE", "size": "BIGINT", "total": "BIGINT",
                     "rate": "DOUBLE", "loud": "VARCHAR"}
    # the union reads them as attributes of that type, so a query over the union compares and does arithmetic as that type
    union = first.concatenate(first, all=True)
    for name, kind in (("doubled", IntegerAttribute), ("half", DoubleAttribute), ("name", StringAttribute),
                       ("size", IntegerAttribute), ("loud", StringAttribute)):
        assert isinstance(union[name], kind), name
