"""A milestoning scheme can be defined in a Schema section, which its tables use before the data store's."""
import pytest
from datafinder_examples import example_path
from mapping_markdown.markdown_mapping import load, save
from mapping_markdown import export_json
from model.mapping import BiTemporalMilestonePropertyMapping


def _mapping():
    return load(str(example_path("schema_milestoning_mapping.md")))


def _milestone(mapping, class_name):
    found = next(m for m in mapping.mappings if m.clazz.name == class_name).milestone_mapping
    assert isinstance(found, BiTemporalMilestonePropertyMapping)
    return found


def test_a_table_uses_the_scheme_of_its_schema_before_the_data_stores():
    quote = _milestone(_mapping(), "Quote")
    assert quote._in.target.name == "recorded_from" and quote._date_from.target.name == "valid_from"
    assert (quote.processing_infinite_datetime, quote.business_infinite_datetime) == (None, "9999-12-31")


def test_a_table_of_a_schema_with_no_scheme_of_that_name_uses_the_data_stores():
    rate = _milestone(_mapping(), "Rate")
    assert rate._in.target.name == "in_z" and rate._date_from.target.name == "from_z"
    assert (rate.processing_infinite_datetime, rate.business_infinite_datetime) == ("9999-12-31 00:00:00", None)


def test_the_schemes_of_each_scope_are_kept_where_they_are_defined():
    from model.relational import Schema
    datastore = _mapping().mappings[0].property_mappings[0].target.table.schema.datastore
    schemas = {each.name: each for each in datastore.schemas}
    assert [s.name for s in datastore.milestoning_schemes] == ["bitemporal"]
    assert [s.name for s in schemas["mkt"].milestoning_schemes] == ["bitemporal"]
    assert schemas["ref"].milestoning_schemes == []
    assert datastore.milestoning_scheme_named("bitemporal", schemas["mkt"]) is schemas["mkt"].milestoning_schemes[0]
    assert datastore.milestoning_scheme_named("bitemporal", schemas["ref"]) is datastore.milestoning_schemes[0]
    assert datastore.milestoning_scheme_named("bitemporal") is datastore.milestoning_schemes[0]
    assert datastore.milestoning_scheme_named("nosuch", schemas["mkt"]) is None
    assert isinstance(schemas["mkt"], Schema)


def test_each_scope_survives_writing(tmp_path):
    path = tmp_path / "mapping.md"
    save(str(path), "Schema Milestoning Mapping", _mapping(), [str(example_path("schema_milestoning_model.md"))])
    text = path.read_text()
    assert text.count("| bitemporal ") == 2  # one in the data store, one in the schema that has its own
    again = load(str(path))
    assert _milestone(again, "Quote")._in.target.name == "recorded_from"
    assert _milestone(again, "Rate")._in.target.name == "in_z"


def test_a_scheme_defined_twice_in_one_scope_is_refused(tmp_path):
    source = example_path("schema_milestoning_mapping.md").read_text()
    row = "| bitemporal | recorded_from    | recorded_to    |               | valid_from         | valid_to         |                              | 9999-12-31                 |"
    assert row in source
    path = tmp_path / "mapping.md"
    path.write_text(source.replace(row, row + "\n" + row).replace("## Model: schema_milestoning_model.md",
                                                                  f"## Model: {example_path('schema_milestoning_model.md')}"))
    with pytest.raises(ValueError, match="Schema 'mkt': the milestoning scheme 'bitemporal' is defined twice"):
        load(str(path))


def test_the_json_export_has_the_schemes_of_each_schema():
    path = example_path("schema_milestoning_mapping.md")
    exported = export_json.export_mapping(str(path), path.name, str(path.parent))
    schemas = {schema["name"]: schema for schema in exported["schemas"]}
    assert [m["name"] for m in exported["dataStore"]["milestoningSchemes"]] == ["bitemporal"]
    assert [(m["name"], m["processingStart"], m["businessInfiniteDatetime"]) for m in schemas["mkt"]["milestoningSchemes"]] == [
        ("bitemporal", "recorded_from", "9999-12-31")]
    assert schemas["ref"]["milestoningSchemes"] == []
