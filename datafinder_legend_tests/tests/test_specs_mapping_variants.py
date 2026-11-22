"""
Runs the specs of the mappings that each have a few tests (bitemporal types on tables milestoned less than the types
are, joins with conditions and chains) against the generated finders: the SQL they generate is compared with Legend's, and the rows they return with the rows Legend's SQL returns on
Legend's test data.
"""
import importlib

import pytest

from datafinder_legend_tests import legend_path
from datafinder_legend_tests.data import seeded_database
from datafinder_legend_tests.finders import generated_finders
from datafinder_legend_tests.gaps import checkable, gap_is_only
from datafinder_legend_tests.legend import BACKEND, discover_specs
from datafinder_legend_tests.results import assert_same_rows

# mapping -> the data it runs on
_VARIANTS = {
    "tests/mapping/union/union_chained_2sets_v1_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_2sets_v2_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_2sets_v3_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_2sets_v4_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_2sets_v5_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_3sets_v1_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_3sets_v2_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_3sets_v3_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_3sets_v4_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_3sets_v5_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_4sets_v1_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_4sets_v2_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_4sets_v3_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_4sets_v4_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_chained_4sets_v5_mapping.md": "tests/mapping/union/union_chained_data.sql",
    "tests/mapping/union/union_filter_mapping.md": "tests/mapping/union/union_data.sql",
    "tests/mapping/union/union_to_union_extra_mapping.md": "tests/mapping/union/union_data.sql",
    "tests/mapping/union/union_same_join_mapping.md": "tests/mapping/union/union_data.sql",
    "tests/mapping/groupBy/group_by_filter_mapping.md": "tests/mapping/groupBy/group_by_data.sql",
    "tests/mapping/groupBy/group_by_two_mapping.md": "tests/mapping/groupBy/group_by_data.sql",
    "tests/mapping/filter/org_filter_mapping.md": "tests/mapping/filter/org_data.sql",
    "tests/mapping/filter/org_filter_join_mapping.md": "tests/mapping/filter/org_data.sql",
    "tests/mapping/groupBy/group_by_mapping.md": "tests/mapping/groupBy/group_by_data.sql",
    "milestoning/tests/propagation_mapping.md": "milestoning/tests/propagation_data.sql",
    "tests/mapping/union/union_partial_address_mapping.md": "tests/mapping/union/union_data.sql",
    "tests/mapping/union/union_partial_firm_key_mapping.md": "tests/mapping/union/union_data.sql",
    "milestoning/tests/business_snapshot_union_mapping.md": "milestoning/tests/business_snapshot_data.sql",
    "milestoning/tests/processing_snapshot_union_mapping.md": "milestoning/tests/processing_snapshot_data.sql",
    "tests/mapping/union/union_merging_mapping.md": "tests/mapping/union/union_merging_data.sql",
    "tests/mapping/union/union_application_embedded_mapping.md": "tests/mapping/union/union_application_embedded_data.sql",
    "tests/mapping/union/union_application_mapping.md": "tests/mapping/union/union_application_data.sql",
    "tests/mapping/union/union_mapping.md": "tests/mapping/union/union_data.sql",
    "tests/mapping/union/union_single_property_mapping.md": "tests/mapping/union/union_data.sql",
    "tds/tests/tds_join_filter_mapping.md": "tests/simple_data.sql",
    "tds/tests/tds_join_mapping.md": "tests/simple_data.sql",
    "milestoning/tests/bitemporal_none_mapping.md": "milestoning/tests/bitemporal_none_data.sql",
    "milestoning/tests/bitemporal_single_mapping.md": "milestoning/tests/business_date_data.sql",
    "tests/mapping/join/advanced_join_mapping.md": "tests/mapping/join/advanced_join_data.sql",
    "tests/mapping/join/chained_joins_mapping.md": "tests/mapping/join/advanced_join_data.sql",
    "tests/mapping/distinct/distinct_mapping.md": "tests/mapping/distinct/distinct_data.sql",
    "tests/mapping/distinct/distinct_filter_mapping.md": "tests/mapping/distinct/distinct_data.sql",
    "tests/mapping/distinct/distinct_join_mapping.md": "tests/mapping/distinct/distinct_data.sql",
    "tests/mapping/classMappingFilterWithInnerJoin/inner_join_filter_mapping.md": "tests/simple_data.sql",
    "tests/mapping/classMappingFilterWithInnerJoin/inner_join_filter_source_view_mapping.md": "tests/simple_data.sql",
    "tests/mapping/classMappingFilterWithInnerJoin/inner_join_filter_target_view_mapping.md": "tests/simple_data.sql",
    "tests/mapping/classMappingFilterWithInnerJoin/inner_join_filter_view_to_view_mapping.md": "tests/simple_data.sql",
    "tests/mapping/distinct/distinct_dup_mapping.md": "tests/mapping/distinct/distinct_data.sql",
    "tests/mapping/distinct/distinct_case_mapping.md": "tests/mapping/distinct/distinct_data.sql",
    "tests/mapping/distinct/distinct_denorm_mapping.md": "tests/mapping/distinct/distinct_data.sql",
}
_ENTRIES = [(mapping, spec, e) for mapping in _VARIANTS for spec in discover_specs(mapping) for e in spec.expectations]
_CASES = [pytest.param(mapping, spec, e, id=f"{spec.finder_name}-{e.name}") for mapping, spec, e in _ENTRIES]


def _finder(mapping: str, finder_name: str):
    """``BiTemporalProductFinder`` from ``bitemporalproduct_finder``, which the generator wrote."""
    module = importlib.import_module(finder_name.removesuffix("Finder").lower() + "_finder")
    return getattr(module, finder_name)()


@pytest.mark.parametrize("mapping,spec,expectation", _CASES)
def test_mapping_variant(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)):
        expectation.run(_finder(mapping, spec.finder_name), backend=BACKEND)


@pytest.mark.parametrize("mapping,spec,expectation", _CASES)
def test_mapping_variant_rows(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)), seeded_database(legend_path(_VARIANTS[mapping])) as database:
        assert_same_rows(database, _finder(mapping, spec.finder_name), expectation)


_GAP_CASES = [pytest.param(mapping, spec, e, id=f"{spec.finder_name}-{e.name}") for mapping, spec, e in _ENTRIES
              if checkable(e.known_gaps.get(BACKEND, ""))]


@pytest.mark.parametrize("mapping,spec,expectation", _GAP_CASES)
def test_known_sql_gap_is_only_what_it_says(mapping, spec, expectation):
    with generated_finders(legend_path(mapping)):
        actual = expectation.query(_finder(mapping, spec.finder_name)).to_sql()
    assert gap_is_only(expectation.known_gaps[BACKEND], actual, expectation.equivalent_sql[BACKEND]), (
        f"[{expectation.name}] differs from Legend by more than: {expectation.known_gaps[BACKEND]}\n"
        f"Actual: {actual}\nExpected: {expectation.equivalent_sql[BACKEND]}")
