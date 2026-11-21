"""Runs COMPANY_FINDER_SPECS and COMPANIES_CONSTANT_SPECS (count, sort, group_by, limit, string and date functions of a constant, window functions)
against Spark."""
import pytest

from datafinder_examples_tests.companies_constant_specs import COMPANIES_CONSTANT_SPECS
from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS
from datafinder_examples_tests.tables import COMPANIES
from spark_harness import finder_of


@pytest.fixture(scope="module")
def company_finder(spark):
    with finder_of(spark, COMPANY_FINDER_SPECS.mapping_file, COMPANIES, "company_finder", "CompanyFinder") as finder:
        yield finder


@pytest.mark.parametrize("expectation", [pytest.param(each, id=each.name) for each in COMPANY_FINDER_SPECS.expectations])
def test_company(expectation, company_finder):
    expectation.run(company_finder, backend="spark")


@pytest.mark.parametrize("expectation", [pytest.param(each, id=each.name) for each in COMPANIES_CONSTANT_SPECS.expectations])
def test_company_constant_functions(expectation, company_finder):
    expectation.run(company_finder, backend="spark")
