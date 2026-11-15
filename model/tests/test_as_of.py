"""The dates a query reads its rows as of, and what they add to what was asked of them."""
import dataclasses
import datetime

import pytest

from model.milestoning import AsOf
from model.relational import ComparisonOperation, ComparisonOperator, IntegerConstantOperation, NoOperation, QuerySpecification, Table

_TABLE = Table("accounts", [("ID", "INT")])


def test_a_query_is_as_of_no_dates_until_it_is_given_some():
    assert QuerySpecification(list(_TABLE.columns), NoOperation(), table=_TABLE).as_of is None
    assert AsOf() == AsOf(None, None, None, None, None)


def test_as_of_never_changes():
    as_of = AsOf(business_date=datetime.date(2024, 1, 2))
    with pytest.raises(dataclasses.FrozenInstanceError):
        as_of.business_date = datetime.date(2025, 1, 1)  # type: ignore[misc]
    assert dataclasses.replace(as_of, business_date_to=datetime.date(2024, 2, 1)).business_date == datetime.date(2024, 1, 2)


def test_what_the_dates_add_is_an_expression_of_the_query():
    added = ComparisonOperation(_TABLE.columns[0], ComparisonOperator.EQUAL, IntegerConstantOperation(1))
    query = QuerySpecification(list(_TABLE.columns), NoOperation(), table=_TABLE)
    assert added not in query.expressions()
    query.as_of = AsOf(filter=added)
    assert added in query.expressions()
    query.as_of = AsOf(business_date=datetime.date(2024, 1, 2))
    assert len(query.expressions()) == len(QuerySpecification(list(_TABLE.columns), NoOperation(), table=_TABLE).expressions())
