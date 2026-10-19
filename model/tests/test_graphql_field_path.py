import pytest

from model.graphql_mapping import PathSegment, association_segment, format_path_segment, parse_field_path, plain_field_path


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
