"""A table can map columns to the ids of the parts of milestoning (validFrom, businessFrom, ...) in place of its scheme's, and give its own infinities."""
from pathlib import Path
from typing import TypeVar

import pytest
from datafinder_examples import example_path
from mapping_markdown.markdown_mapping import load, save
from model.mapping import BiTemporalMilestonePropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping

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


def test_a_table_that_overrides_nothing_has_its_scheme():
    quote = _milestone(_mapping(), "Quote", BiTemporalMilestonePropertyMapping)
    assert (quote._in.target.name, quote._out.target.name) == ("in_z", "out_z")
    assert (quote._date_from.target.name, quote._date_to.target.name) == ("from_z", "thru_z")
    assert (quote.processing_infinite_datetime, quote.business_infinite_datetime) == ("9999-12-31 00:00:00", None)


def test_a_table_has_the_business_range_it_maps_and_the_processing_of_its_scheme():
    rate = _milestone(_mapping(), "Rate", BiTemporalMilestonePropertyMapping)
    assert (rate._in.target.name, rate._out.target.name) == ("in_z", "out_z")
    assert (rate._date_from.target.name, rate._date_to.target.name) == ("valid_from", "valid_to")


def test_a_business_date_replaces_the_business_range_of_the_scheme():
    fee = _milestone(_mapping(), "Fee", BusinessDateAndProcessingMilestonePropertyMapping)
    assert fee._date.target.name == "fee_date"
    assert (fee._in.target.name, fee._out.target.name) == ("in_z", "out_z")
    assert fee.processing_infinite_datetime == "9999-12-31 00:00:00"


def test_a_heading_gives_the_infinity_of_an_axis_in_place_of_the_schemes():
    rate = _milestone(_mapping(), "Rate", BiTemporalMilestonePropertyMapping)
    assert (rate.processing_infinite_datetime, rate.business_infinite_datetime) == ("9999-12-31 00:00:00", "9999-12-31")


def test_a_heading_infinity_of_null_is_a_null_end(tmp_path):
    source = _source().replace("(milestoning: bitemporal, business_infinity: 9999-12-31)", "(milestoning: bitemporal, processing_infinity: null)")
    rate = _milestone(_mapping_of(source, tmp_path), "Rate", BiTemporalMilestonePropertyMapping)
    assert (rate.processing_infinite_datetime, rate.business_infinite_datetime) == (None, None)


def test_a_single_column_of_a_dimension_can_be_overridden(tmp_path):
    source = _source().replace("| out_z      | TIMESTAMP |     | Y        | validTo      |", "| ended_z    | TIMESTAMP |     | Y        | validTo      |")
    rate = _milestone(_mapping_of(source, tmp_path), "Rate", BiTemporalMilestonePropertyMapping)
    assert (rate._in.target.name, rate._out.target.name) == ("in_z", "ended_z")  # the end only; the start is the scheme's


def test_a_table_that_names_no_scheme_is_milestoned_by_the_columns_it_maps(tmp_path):
    source = _source().replace("#### Table: fees → Fee (milestoning: bitemporal)", "#### Table: fees → Fee (processing_infinity: 9999-01-01 00:00:00)")
    fee = _milestone(_mapping_of(source, tmp_path), "Fee", BusinessDateAndProcessingMilestonePropertyMapping)
    assert (fee._date.target.name, fee._in.target.name, fee.processing_infinite_datetime) == ("fee_date", "in_z", "9999-01-01 00:00:00")


def test_a_range_needs_both_of_its_ids(tmp_path):
    source = _source().replace("| valid_to   | DATE      |     | Y        | businessThru |\n", "").replace(
        "(milestoning: bitemporal, business_infinity: 9999-12-31)", "(processing_infinity: null)")
    with pytest.raises(ValueError, match="a business range: businessFrom and businessThru are both needed"):
        _mapping_of(source, tmp_path)


def test_a_business_date_and_a_business_range_are_not_both_mapped(tmp_path):
    source = _source().replace("| fee_date | DATE      |     | Y        | businessDate |\n",
                               "| fee_date | DATE      |     | Y        | businessDate |\n| from_z   | DATE      |     | Y        | businessFrom |\n")
    with pytest.raises(ValueError, match="a table has a business date .* or a business range .*, not both"):
        _mapping_of(source, tmp_path)


def test_two_columns_are_not_mapped_to_one_id(tmp_path):
    source = _source().replace("| fee_date | DATE      |     | Y        | businessDate |\n",
                               "| fee_date | DATE      |     | Y        | businessDate |\n| other    | DATE      |     | Y        | businessDate |\n")
    with pytest.raises(ValueError, match="the columns 'fee_date' and 'other' are both mapped to 'businessDate'"):
        _mapping_of(source, tmp_path)


def test_an_infinity_is_not_given_to_a_table_that_is_not_milestoned(tmp_path):
    source = _source().replace("#### Table: fees → Fee (milestoning: bitemporal)", "#### Table: fees → Fee (business_infinity: 9999-12-31)").replace(
        "| in_z     | TIMESTAMP |     | Y        | validFrom    |\n| out_z    | TIMESTAMP |     | Y        | validTo      |\n| fee_date | DATE      |     | Y        | businessDate |\n", "")
    with pytest.raises(ValueError, match="an infinity is given for a table that is not milestoned"):
        _mapping_of(source, tmp_path)


def test_each_table_survives_writing(tmp_path):
    path = tmp_path / "mapping.md"
    save(str(path), "Table Milestoning Mapping", _mapping(), [str(example_path(_MODEL))])
    written = path.read_text()
    # a table that overrides names its scheme and says only what is not the scheme's
    assert "→ Quote (milestoning: bitemporal)" in written
    assert "→ Rate (milestoning: bitemporal, business_infinity: 9999-12-31)" in written
    assert "→ Fee (milestoning: bitemporal)" in written
    again = load(str(path))
    rate = _milestone(again, "Rate", BiTemporalMilestonePropertyMapping)
    fee = _milestone(again, "Fee", BusinessDateAndProcessingMilestonePropertyMapping)
    assert (rate._date_from.target.name, rate.business_infinite_datetime) == ("valid_from", "9999-12-31")
    assert (fee._date.target.name, fee._in.target.name) == ("fee_date", "in_z")
    assert _milestone(again, "Quote", BiTemporalMilestonePropertyMapping)._date_from.target.name == "from_z"


def test_the_scheme_a_table_makes_of_its_own_columns_is_one_of_the_supported_milestonings(tmp_path):
    # a snapshot replaces the processing range of the scheme, which still has its business range: a snapshot is a scheme on its own
    source = _source().replace("| in_z       | TIMESTAMP |     | Y        | validFrom    |\n| out_z      | TIMESTAMP |     | Y        | validTo      |\n",
                               "| taken_at   | TIMESTAMP |     | Y        | processingDatetime |\n")
    with pytest.raises(ValueError, match="Table 'rates' → 'Rate': milestoning scheme 'bitemporal' has a processing_datetime, a processing snapshot, "
                                         "which cannot be combined with business_date_from, business_date_to"):
        _mapping_of(source, tmp_path)
