"""The GraphQL engine pushes filters, sorts and limits to the server and fails when it cannot."""
import pandas as pd
import pytest

from datafinder.typed_attributes import DoubleAttribute, StringAttribute
from datafinder_graphql.graphql_engine import GraphQLUnsupportedError, GraphQLPushdownError, _build_server_args, _project_columns
from model.graphql_mapping import GraphQLFilterConvention
from model.relational import (
    ComparisonOperation, ComparisonOperator, LogicalOperation, LogicalOperator, WindowFunction, WindowFunctionOperation,
)

CONVENTION = GraphQLFilterConvention("where", "order_by", "limit")


def _name() -> StringAttribute:
    return StringAttribute("Account Name", "account.name", "STRING", "trades")


def _price() -> DoubleAttribute:
    return DoubleAttribute("Price", "price", "DOUBLE", "trades")


def test_top_level_filter_is_pushed():
    assert _build_server_args(_price().eq(1.5), None, None, CONVENTION) == ["where: { price: { _eq: 1.5 } }"]


def test_nested_filter_is_pushed_as_nested_objects():
    args = _build_server_args(_name().eq("Acme"), None, None, CONVENTION)
    assert args == ['where: { account: { name: { _eq: "Acme" } } }']


def test_nested_sort_is_pushed_as_nested_objects():
    args = _build_server_args(None, [_name().ascending()], None, CONVENTION)
    assert args == ["order_by: [{ account: { name: asc } }]"]


def test_string_prefix_is_pushed_as_like():
    args = _build_server_args(_name().starts_with("Ac"), None, None, CONVENTION)
    assert args == ['where: { account: { name: { _like: "Ac%" } } }']


def _field_compared_to_field() -> ComparisonOperation:
    return ComparisonOperation(_price()._column_with_join(), ComparisonOperator.EQUAL, _price()._column_with_join())


def test_limit_is_pushed():
    assert _build_server_args(None, None, 5, CONVENTION) == ["limit: 5"]


def test_nothing_to_push_needs_no_convention():
    assert _build_server_args(None, None, None, None) == []


@pytest.mark.parametrize("op, order_by, limit", [
    (_price().eq(1.5), None, None),
    (None, [_price().ascending()], None),
    (None, None, 5),
])
def test_pushing_without_a_filter_convention_fails(op, order_by, limit):
    with pytest.raises(GraphQLPushdownError, match="no filter convention"):
        _build_server_args(op, order_by, limit, None)


def test_or_with_an_unpushable_side_fails_rather_than_narrowing():
    with pytest.raises(GraphQLPushdownError):
        _build_server_args(LogicalOperation(_price().eq(2.0), LogicalOperator.OR, _field_compared_to_field()), None, None, CONVENTION)


def test_and_with_an_unpushable_side_fails():
    with pytest.raises(GraphQLPushdownError):
        _build_server_args(_price().eq(2.0).and_op(_field_compared_to_field()), None, None, CONVENTION)


def test_an_unsupported_column_expression_fails_rather_than_disappearing():
    rank = WindowFunctionOperation(_price()._column_with_join(), WindowFunction.RANK, "Rank")
    with pytest.raises(GraphQLUnsupportedError, match="WindowFunctionOperation"):
        _project_columns(pd.DataFrame({"price": [1.0]}), [rank])
