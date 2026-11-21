"""Runs COMPANY_FINDER_SPECS and COMPANIES_CONSTANT_SPECS (count, sort, group_by, limit, string and date functions of a constant, window functions)
against Spark."""
import pytest

from datafinder_examples_tests.companies_constant_specs import COMPANIES_CONSTANT_SPECS
from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS
from datafinder_examples_tests.tables import COMPANIES
from spark_harness import DATE_MINUS_DATE, DATE_PLUS_INTERVAL, DISTINCT_ORDER, EPOCH, ISODOW, LISTAGG, expecting, finder_of

_GAPS = {
    **{name: LISTAGG for name in (
        "join_per_group_in_an_order", "join_descending", "join_of_the_whole_result", "join_without_an_order_joins_every_value",
        "having_on_joined_text")},
    **{name: DISTINCT_ORDER for name in (
        "distinct_removes_repeated_rows", "distinct_keeps_rows_that_differ_in_any_column", "distinct_with_a_filter",
        "distinct_applies_before_the_limit", "offset_applies_after_distinct")},
}
_CONSTANT_GAPS = {
    **{each.name: ISODOW for each in COMPANIES_CONSTANT_SPECS.expectations if each.name in ("date_constant_parts", "datetime_constant_parts")},
    **{each.name: EPOCH for each in COMPANIES_CONSTANT_SPECS.expectations
       if each.name == "datetime_constant_difference" or each.name.startswith("datetime_difference_counts_the_boundaries_crossed_")},
    **{each.name: DATE_MINUS_DATE for each in COMPANIES_CONSTANT_SPECS.expectations
       if each.name == "date_constant_difference" or each.name.startswith("date_difference_counts_the_boundaries_crossed_")},
    "date_constant_moved": DATE_PLUS_INTERVAL,
}


@pytest.fixture(scope="module")
def company_finder(spark):
    with finder_of(spark, COMPANY_FINDER_SPECS.mapping_file, COMPANIES, "company_finder", "CompanyFinder") as finder:
        yield finder


@pytest.mark.parametrize("expectation", expecting(COMPANY_FINDER_SPECS.expectations, _GAPS))
def test_company(expectation, company_finder):
    expectation.run(company_finder, backend="spark")


@pytest.mark.parametrize("expectation", expecting(COMPANIES_CONSTANT_SPECS.expectations, _CONSTANT_GAPS))
def test_company_constant_functions(expectation, company_finder):
    expectation.run(company_finder, backend="spark")
