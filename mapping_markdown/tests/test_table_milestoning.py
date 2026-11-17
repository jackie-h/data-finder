"""A table can map columns to the ids of the parts of milestoning (validFrom, businessFrom, ...) in place of its scheme's, and give its own infinities."""
from pathlib import Path
from typing import TypeVar

import pytest
from datafinder_examples import example_path
from mapping_markdown.markdown_mapping import load, save
from model.mapping import (
    BiTemporalMilestonePropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping, ProcessingDateMilestonesPropertyMapping)

_Milestone = TypeVar("_Milestone")
_MAPPING = "table_milestoning_mapping.md"
_MODEL = "table_milestoning_model.md"


def _mapping():
    return load(str(example_path(_MAPPING)))


def _mapping_of(source: str, tmp_path: Path):
    path = tmp_path / "mapping.md"
    path.write_text(source.replace(f"## Model: {_MODEL}", f"## Model: {example_path(_MODEL)}"))
    return load(str(path))


def _source() -> str:
    return example_path(_MAPPING).read_text()


def _milestone(mapping, class_name: str, kind: type[_Milestone]) -> _Milestone:
    found = next(m for m in mapping.mappings if m.clazz.name == class_name).milestone_mapping
    assert isinstance(found, kind), found
    return found


def test_a_table_has_the_business_range_it_maps_beside_the_processing_of_its_scheme():
    quote = _milestone(_mapping(), "Quote", BiTemporalMilestonePropertyMapping)
    assert (quote._in.target.name, quote._out.target.name) == ("in_z", "out_z")
    assert (quote._date_from.target.name, quote._date_to.target.name) == ("from_z", "thru_z")


def test_a_table_has_the_business_date_it_maps():
    rate = _milestone(_mapping(), "Rate", BusinessDateAndProcessingMilestonePropertyMapping)
    assert rate._date.target.name == "rate_date"


def test_a_column_mapped_to_validFrom_is_used_in_place_of_the_schemes():
    fee = _milestone(_mapping(), "Fee", ProcessingDateMilestonesPropertyMapping)
    assert (fee._in.target.name, fee._out.target.name) == ("recorded_in", "recorded_to")
    assert fee.processing_infinite_datetime == "9999-12-31 00:00:00"  # the infinity is the scheme's


def test_a_heading_gives_the_infinity_of_an_axis_the_scheme_has_none_for():
    mapping = _mapping()
    quote, rate = _milestone(mapping, "Quote", BiTemporalMilestonePropertyMapping), _milestone(mapping, "Rate", BusinessDateAndProcessingMilestonePropertyMapping)
    assert (quote.processing_infinite_datetime, quote.business_infinite_datetime) == ("9999-12-31 00:00:00", "9999-12-31")
    assert rate.processing_infinite_datetime == "9999-12-31 00:00:00"


def test_a_heading_infinity_of_null_is_a_null_end(tmp_path):
    source = _source().replace("(milestoning: standard, business_infinity: 9999-12-31)", "(milestoning: standard, processing_infinity: null)")
    quote = _milestone(_mapping_of(source, tmp_path), "Quote", BiTemporalMilestonePropertyMapping)
    assert (quote.processing_infinite_datetime, quote.business_infinite_datetime) == (None, None)


def test_a_table_that_names_no_scheme_is_milestoned_by_the_columns_it_maps(tmp_path):
    source = _source().replace("(milestoning: standard)\n\n| Column      |", "(processing_infinity: 9999-01-01 00:00:00)\n\n| Column      |")
    fee = _milestone(_mapping_of(source, tmp_path), "Fee", ProcessingDateMilestonesPropertyMapping)
    assert (fee._in.target.name, fee.processing_infinite_datetime) == ("recorded_in", "9999-01-01 00:00:00")


def test_a_range_needs_both_of_its_ids(tmp_path):
    source = _source().replace("| thru_z | DATE      |     | Y        | businessThru |\n", "")
    with pytest.raises(ValueError, match="a business range: businessFrom and businessThru are both needed"):
        _mapping_of(source, tmp_path)


def test_a_business_date_and_a_business_range_are_not_both_mapped(tmp_path):
    source = _source().replace("| thru_z | DATE      |     | Y        | businessThru |\n",
                               "| thru_z | DATE      |     | Y        | businessThru |\n| as_of  | DATE      |     | Y        | businessDate |\n")
    with pytest.raises(ValueError, match="a business date or a business range"):
        _mapping_of(source, tmp_path)


def test_two_columns_are_not_mapped_to_one_id(tmp_path):
    source = _source().replace("| recorded_to | TIMESTAMP |     | Y        | validTo     |\n",
                               "| recorded_to | TIMESTAMP |     | Y        | validTo     |\n| other_to    | TIMESTAMP |     | Y        | validTo     |\n")
    with pytest.raises(ValueError, match="the columns 'recorded_to' and 'other_to' are both mapped to 'validTo'"):
        _mapping_of(source, tmp_path)


def test_an_infinity_is_not_given_to_a_table_that_is_not_milestoned(tmp_path):
    source = _source().replace("(milestoning: standard)\n\n| Column    |", "(business_infinity: 9999-12-31)\n\n| Column    |").replace(
        "| in_z      | TIMESTAMP |     | Y        | validFrom    |\n| out_z     | TIMESTAMP |     | Y        | validTo      |\n| rate_date | DATE      |     | Y        | businessDate |\n", "")
    with pytest.raises(ValueError, match="an infinity is given for a table that is not milestoned"):
        _mapping_of(source, tmp_path)


def test_each_table_survives_writing(tmp_path):
    path = tmp_path / "mapping.md"
    save(str(path), "Table Milestoning Mapping", _mapping(), [str(example_path(_MODEL))])
    written = path.read_text()
    # a table that adds to its scheme names it, and says only what is not the scheme's; one that has other columns than the scheme's says its own
    assert "→ Quote (milestoning: standard, business_infinity: 9999-12-31)" in written
    assert "→ Rate (milestoning: standard)" in written
    assert "→ Fee (processing_infinity: 9999-12-31 00:00:00)" in written
    again = load(str(path))
    quote, rate, fee = (_milestone(again, "Quote", BiTemporalMilestonePropertyMapping), _milestone(again, "Rate", BusinessDateAndProcessingMilestonePropertyMapping), _milestone(again, "Fee", ProcessingDateMilestonesPropertyMapping))
    assert quote.business_infinite_datetime == "9999-12-31"
    assert quote.processing_infinite_datetime == "9999-12-31 00:00:00"
    assert rate._date.target.name == "rate_date" and fee._in.target.name == "recorded_in"
