"""A milestoning scheme is one of the supported milestonings, and not any set of columns."""
import pytest

from model.relational import MilestoningScheme


@pytest.mark.parametrize("columns, kind", [
    (dict(processing_start="in_z", processing_end="out_z"), "processing_only"),
    (dict(business_date="d"), "business_date"),
    (dict(business_date="d", processing_start="in_z", processing_end="out_z"), "business_date_processing"),
    (dict(business_date_from="f", business_date_to="t", processing_start="in_z", processing_end="out_z"), "bitemporal"),
    (dict(business_date_from="f", business_date_to="t"), "business_date_range"),
    (dict(processing_datetime="taken_at"), "processing_snapshot"),
])
def test_each_supported_set_of_columns_is_a_kind(columns, kind):
    assert MilestoningScheme("any name", **columns).kind() == kind


@pytest.mark.parametrize("columns, message", [
    (dict(), "has no columns"),
    (dict(processing_start="in_z"), "has both processing_start and processing_end"),
    (dict(processing_end="out_z"), "has both processing_start and processing_end"),
    (dict(business_date_from="f"), "has both business_date_from and business_date_to"),
    (dict(business_date_to="t", business_date="d"), "has both business_date_from and business_date_to"),
    (dict(business_date="d", business_date_from="f", business_date_to="t"), "a business date .* or a business range .*, not both"),
    (dict(processing_datetime="taken_at", processing_start="in_z", processing_end="out_z"),
     "cannot be combined with processing_start, processing_end"),
    (dict(processing_datetime="taken_at", business_date="d"), "cannot be combined with business_date"),
])
def test_any_other_set_of_columns_is_refused(columns, message):
    with pytest.raises(ValueError, match=message):
        MilestoningScheme("odd", **columns).kind()
