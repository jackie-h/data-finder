"""The GraphQL engine pushes filters, sorts and limits to the server and fails when it cannot."""
import pandas as pd
import pytest

from datafinder.typed_attributes import DoubleAttribute, StringAttribute
from datafinder_graphql.graphql_engine import (
    GraphQLPushdownError, GraphQLUnsupportedError, _build_fields_str, _build_server_args, _check_expansion,
    _flatten_rows, _project_columns, _sort_expanded,
)
from model.graphql_mapping import GraphQLFilterConvention, format_path_segment
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


def _projects_name() -> StringAttribute:
    return StringAttribute("Project Name", "projects[].name", "STRING", "employees")


def test_a_to_many_relation_expands_to_a_row_per_child():
    rows = _flatten_rows({"name": "Bob", "projects": [{"name": "Alpha"}, {"name": "Beta"}]})
    assert rows == [{"name": "Bob", "projects.name": "Alpha"}, {"name": "Bob", "projects.name": "Beta"}]


def test_an_empty_to_many_relation_keeps_the_parent_without_the_fields():
    assert _flatten_rows({"name": "Alice", "projects": []}) == [{"name": "Alice"}]


def test_a_to_many_filter_is_pushed_as_a_nested_object_without_the_marker():
    args = _build_server_args(_projects_name().eq("Alpha"), None, None, CONVENTION)
    assert args == ['where: { projects: { name: { _eq: "Alpha" } } }']


def test_a_sort_on_a_to_many_relation_is_not_pushed():
    with pytest.raises(GraphQLPushdownError, match="to-many"):
        _build_server_args(None, [_projects_name().ascending()], None, CONVENTION)


def test_filtering_and_selecting_the_same_to_many_relation_fails():
    with pytest.raises(GraphQLUnsupportedError, match="same to-many relation"):
        _check_expansion(_projects_name().eq("Alpha"), [_projects_name()], None)


def test_a_limit_on_rows_expanded_by_a_to_many_relation_fails():
    with pytest.raises(GraphQLUnsupportedError, match="limit"):
        _check_expansion(None, [_projects_name()], 5)


def test_expanded_rows_are_sorted_after_expansion():
    df = pd.DataFrame({"projects.name": ["Beta", "Alpha"], "name": ["Bob", "Bob"]})
    assert list(_sort_expanded(df, [_projects_name().ascending()])["projects.name"]) == ["Alpha", "Beta"]


def _filtered_projects_name(**filters) -> StringAttribute:
    path = format_path_segment("projects", True, filters) + ".name"
    return StringAttribute("Project Name", path, "STRING", "employees")


def test_a_filtered_to_many_relation_takes_a_where_argument():
    fields = _build_fields_str([_filtered_projects_name(code="ALPHA")], CONVENTION)
    assert fields == 'projects(where: { code: { _eq: "ALPHA" } }) { name }'


def test_several_filters_on_a_relation_are_anded():
    fields = _build_fields_str([_filtered_projects_name(code="ALPHA", name="Alpha Initiative")], CONVENTION)
    assert fields == ('projects(where: { _and: [{ code: { _eq: "ALPHA" } }, '
                      '{ name: { _eq: "Alpha Initiative" } }] }) { name }')


def test_a_filtered_relation_nested_in_another_keeps_its_arguments():
    path = "manager." + format_path_segment("projects", True, {"code": "ALPHA"}) + ".name"
    column = StringAttribute("Manager Project Name", path, "STRING", "employees")
    assert _build_fields_str([column], CONVENTION) == 'manager { projects(where: { code: { _eq: "ALPHA" } }) { name } }'


def test_a_to_one_relation_cannot_be_filtered():
    path = format_path_segment("account", False, {"name": "Acme"}) + ".name"
    with pytest.raises(GraphQLPushdownError, match="to-one"):
        _build_fields_str([StringAttribute("Account Name", path, "STRING", "trades")], CONVENTION)


def test_filtering_a_relation_needs_a_filter_convention():
    with pytest.raises(GraphQLPushdownError, match="no filter convention"):
        _build_fields_str([_filtered_projects_name(code="ALPHA")], None)


def test_one_relation_selected_with_different_filters_fails():
    columns = [_filtered_projects_name(code="ALPHA"), _filtered_projects_name(code="BETA")]
    with pytest.raises(GraphQLUnsupportedError, match="different filters"):
        _build_fields_str(columns, CONVENTION)
