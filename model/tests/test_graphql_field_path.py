import datetime

import pytest

from model.graphql_mapping import (
    GraphQLBiTemporalMilestone, GraphQLBusinessDateMilestone, GraphQLProcessingMilestone, PathSegment,
    association_segment, format_path_segment, parse_field_path, plain_field_path, temporal_arguments,
)


def test_a_plain_path_has_a_segment_per_field():
    assert parse_field_path("account.branch.city") == [
        PathSegment("account"), PathSegment("branch"), PathSegment("city")]


def test_a_to_many_segment_is_marked():
    assert parse_field_path("projects[].name") == [PathSegment("projects", many=True), PathSegment("name")]


def test_a_segment_carries_its_filters():
    path = "manager." + format_path_segment("projects", True, {"code": "ALPHA"}) + ".name"
    assert parse_field_path(path) == [
        PathSegment("manager"), PathSegment("projects", True, {"code": "ALPHA"}), PathSegment("name")]


def test_a_filter_value_may_contain_path_characters():
    path = format_path_segment("account", False, {"name": "a.b[c]{d}"}) + ".id"
    assert parse_field_path(path) == [PathSegment("account", False, {"name": "a.b[c]{d}"}), PathSegment("id")]


def test_the_plain_path_drops_markers_and_filters():
    path = "manager." + format_path_segment("projects", True, {"code": "ALPHA"}) + ".name"
    assert plain_field_path(path) == "manager.projects.name"


def test_association_filters_are_translated_to_graphql_fields():
    segment = association_segment("projects", True, {"first_name": "Bob"}, {"first_name": "firstName"})
    assert parse_field_path(segment) == [PathSegment("projects", True, {"firstName": "Bob"})]


def test_association_without_filters_has_no_filter_suffix():
    assert association_segment("projects", True, {}, {"code": "code"}) == "projects[]"


def test_an_unknown_association_filter_is_rejected():
    with pytest.raises(TypeError, match="Unknown filter nope"):
        association_segment("projects", True, {"nope": 1}, {"code": "code"})


def test_a_segment_carries_the_arguments_it_is_read_with():
    path = "branch." + format_path_segment("accounts", True, {"name": "Acme"}, {"asOf": "2021-06-01T00:00:00"}) + ".id"
    assert parse_field_path(path) == [
        PathSegment("branch"),
        PathSegment("accounts", True, {"name": "Acme"}, {"asOf": "2021-06-01T00:00:00"}),
        PathSegment("id")]


def test_a_segment_with_only_arguments_has_no_filters():
    (segment,) = parse_field_path(format_path_segment("account", False, None, {"asOf": "2021-06-01T00:00:00"}))
    assert segment.filters == {} and segment.args == {"asOf": "2021-06-01T00:00:00"}


def test_a_filter_on_a_field_named_like_an_argument_is_not_an_argument():
    (segment,) = parse_field_path(format_path_segment("accounts", True, {"asOf": "x"}))
    assert segment.filters == {"asOf": "x"} and segment.args == {}


def test_a_processing_milestone_takes_the_processing_time_as_its_argument():
    args = temporal_arguments("account", GraphQLProcessingMilestone("asOf"), None, datetime.datetime(2021, 6, 1, 9, 30))
    assert args == {"asOf": "2021-06-01T09:30:00"}


def test_a_processing_date_is_midnight():
    args = temporal_arguments("account", GraphQLProcessingMilestone("asOf"), None, datetime.date(2021, 6, 1))
    assert args == {"asOf": "2021-06-01T00:00:00"}


def test_a_bitemporal_milestone_takes_both_dates():
    milestone = GraphQLBiTemporalMilestone("businessDate", "asOf")
    args = temporal_arguments("positions", milestone, datetime.date(2023, 1, 15), "2023-02-01 12:00:00")
    assert args == {"businessDate": "2023-01-15", "asOf": "2023-02-01 12:00:00"}


def test_a_business_date_milestone_takes_a_business_date():
    assert temporal_arguments("x", GraphQLBusinessDateMilestone("on"), "2023-01-15") == {"on": "2023-01-15"}


def test_no_dates_given_means_no_arguments_even_with_no_milestone():
    assert temporal_arguments("branches", None) == {}


def test_a_query_with_no_milestone_takes_no_date():
    with pytest.raises(TypeError, match="branch has no processing time"):
        temporal_arguments("branch", None, None, "2021-06-01")
    with pytest.raises(TypeError, match="branch has no business date"):
        temporal_arguments("branch", None, "2021-06-01")


def test_a_processing_milestone_takes_no_business_date():
    with pytest.raises(TypeError, match="account has no business date"):
        temporal_arguments("account", GraphQLProcessingMilestone("asOf"), "2021-06-01")


def test_association_segment_reads_the_relation_at_the_dates_given():
    segment = association_segment("accounts", True, {}, {}, GraphQLProcessingMilestone("asOf"), None,
                                  datetime.datetime(2021, 6, 1))
    assert parse_field_path(segment) == [PathSegment("accounts", True, {}, {"asOf": "2021-06-01T00:00:00"})]
