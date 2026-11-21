"""Where a sort puts a missing value: the smallest value's place by default, as Legend's databases have it, or as ``nulls`` says."""
import pytest

from datafinder import IntegerAttribute, StringAttribute
from datafinder.sql_generator import to_sql
from model.relational import NoOperation, SortDirection, Table

_TABLE = Table("t", [("id", "INT"), ("category", "VARCHAR")])
_ID = IntegerAttribute("Id", "id", "INT", _TABLE, nullable=False)
_CATEGORY = StringAttribute("Category", "category", "VARCHAR", _TABLE)


def _sql(*sorts) -> str:
    return to_sql(None, None, [_ID, _CATEGORY], _TABLE, NoOperation(), order_by=list(sorts))


def test_a_missing_value_is_the_smallest_by_default():
    assert _CATEGORY.ascending().nulls_first is True
    assert _CATEGORY.descending().nulls_first is False


@pytest.mark.parametrize("make,nulls,first", [
    (lambda a, nulls: a.ascending(nulls=nulls), "last", False),
    (lambda a, nulls: a.ascending(nulls=nulls), "first", True),
    (lambda a, nulls: a.descending(nulls=nulls), "last", False),
    (lambda a, nulls: a.descending(nulls=nulls), "first", True),
])
def test_nulls_says_where_a_missing_value_goes(make, nulls, first):
    assert make(_CATEGORY, nulls).nulls_first is first


@pytest.mark.parametrize("method", ["ascending", "descending"])
def test_nulls_is_first_or_last(method):
    with pytest.raises(ValueError, match="'first' or 'last'"):
        getattr(_CATEGORY, method)(nulls="middle")


def test_the_sql_says_where_a_missing_value_goes_every_time():
    assert _sql(_CATEGORY.ascending()).endswith("ORDER BY t0.category ASC NULLS FIRST")
    assert _sql(_CATEGORY.descending()).endswith("ORDER BY t0.category DESC NULLS LAST")
    assert _sql(_CATEGORY.ascending(nulls="last")).endswith("ORDER BY t0.category ASC NULLS LAST")
    assert _sql(_CATEGORY.descending(nulls="first")).endswith("ORDER BY t0.category DESC NULLS FIRST")


def test_a_column_that_cannot_be_missing_and_an_aggregate_say_it_too():
    assert _sql(_ID.ascending()).endswith("ORDER BY t0.id ASC NULLS FIRST")
    assert to_sql(None, None, [_ID.count()], _TABLE, NoOperation(), order_by=[_ID.count().descending()]).endswith("ORDER BY COUNT(t0.id) DESC NULLS LAST")
    assert to_sql(None, None, [_ID.sum()], _TABLE, NoOperation(), order_by=[_ID.sum().descending()]).endswith("ORDER BY SUM(t0.id) DESC NULLS LAST")


def test_the_direction_is_what_it_was():
    assert _CATEGORY.ascending(nulls="last").direction == SortDirection.ASC
    assert _CATEGORY.descending(nulls="first").direction == SortDirection.DESC
