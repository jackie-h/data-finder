"""
Backend-agnostic test specifications for datafinder.

Import specs and run them against any backend:

    from datafinder_examples_tests.finance_specs import TRADE_FINDER_SPECS
    from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS

    for expectation in TRADE_FINDER_SPECS.expectations:
        expectation.run(trade_finder, backend="duckdb")
"""
from datafinder_examples_tests.spec import TestExpectation, FinderSpec

__all__ = ["TestExpectation", "FinderSpec"]
