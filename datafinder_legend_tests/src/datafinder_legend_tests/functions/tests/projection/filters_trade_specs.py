"""
Ported from legend-engine core_relational ``functions/tests/projection/testFilters.pure``.

Comparison filters on a numeric property.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testFilters.pure"

FILTERS_TRADE_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGreaterThan", f"{_SRC}::testGreaterThan",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity > 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() > 45)),
        ported("testGreaterThanEqual", f"{_SRC}::testGreaterThanEqual",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity >= 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() >= 45)),
        ported("testLessThan", f"{_SRC}::testLessThan",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity < 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() < 45)),
        ported("testLessThanFilterAsVariable", f"{_SRC}::testLessThanFilterAsVariable",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity < 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() < 45)),
        ported("testLessThanEqual", f"{_SRC}::testLessThanEqual",
               'select "root".quantity as "quantity" from tradeTable as "root" where "root".quantity <= 45',
               lambda f: f.find_all(None, None, [f.quantity()], f.quantity() <= 45)),
    ],
)
