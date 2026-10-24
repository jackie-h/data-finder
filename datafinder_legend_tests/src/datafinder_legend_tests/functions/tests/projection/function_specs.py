"""
Ported from legend-engine core_relational ``functions/tests/projection/testFunction.pure``.

Projecting several properties, navigating associations and applying functions.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, CONCAT_FUNCTION, JOIN_OPERAND_ORDER

_SRC = "functions/tests/projection/testFunction.pure"

FUNCTION_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleFunction", f"{_SRC}::testSimpleFunction",
               'select "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testFunctionAndPathOneSide", f"{_SRC}::testFunctionAndPathOneSide",
               'select "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name()])),
        ported("testFunctionAndPathOtherSide", f"{_SRC}::testFunctionAndPathOtherSide",
               'select "root".FIRSTNAME as "firstName", "root".LASTNAME as "lastName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()])),
        ported("testSimpleFunctionWithToOne", f"{_SRC}::testSimpleFunctionWithToOne",
               'select "root".LASTNAME as "lastName", "addresstable_0".NAME as "addressName" from personTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID)',
               lambda f: f.find_all(None, None, [f.last_name(), f.address().name()])),
        ported("testSimpleRealFunction", f"{_SRC}::testSimpleRealFunction",
               'select "root".LASTNAME as "lastName", concat("root".FIRSTNAME, \'|\', "root".LASTNAME) as "fullName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name() + "|" + f.last_name()]),
               known_gaps={BACKEND: CONCAT_FUNCTION}),
    ],
)

FUNCTION_FIRM_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleFunctionWithToManyMap", f"{_SRC}::testSimpleFunctionWithToManyMap",
               'select "root".LEGALNAME as "legalName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().last_name()])),
        ported("testSimpleFunctionWithTwoToManyMap", f"{_SRC}::testSimpleFunctionWithTwoToManyMap",
               'select "root".LEGALNAME as "legalName", "persontable_0".FIRSTNAME as "firstName", "persontable_0".LASTNAME as "lastName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().first_name(), f.employees().last_name()])),
    ],
)

FUNCTION_TRADE_SPECS = FinderSpec(
    finder_name="TradeFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testAbs", f"{_SRC}::testAbs",
               'select abs("root".quantity) as "quantity" from tradeTable as "root" where "root".quantity <= 45',
               lambda f: f.find_all(None, None, [f.quantity().abs()], f.quantity() <= 45)),
        unsupported("testDatePartWithZeroToOne", f"{_SRC}::testDatePartWithZeroToOne",
                    'select cast(truncate("root".settlementDateTime) as date) as "settlementDateTime" from tradeTable as "root" where "root".ID in (9, 10)',
                    'no datePart (truncate to a date)'),
        ported("testWeekOfYearWithZeroToOne", f"{_SRC}::testWeekOfYearWithZeroToOne",
               'select extract(week from "root".settlementDateTime) as "settlementWeekOfYear" from tradeTable as "root" where "root".ID in (9, 10)',
               lambda f: f.find_all(None, None, [f.settlement_date_time().week()], f.id_().is_in([9, 10]))),
        ported("testMonthNumberZeroToOne", f"{_SRC}::testMonthNumberZeroToOne",
               'select extract(month from "root".settlementDateTime) as "settlementMonthNumber" from tradeTable as "root" where "root".ID in (9, 10)',
               lambda f: f.find_all(None, None, [f.settlement_date_time().month()], f.id_().is_in([9, 10]))),
        ported("testDayOfMonth", f"{_SRC}::testDayOfMonth",
               'select "root".settlementDateTime as "Date", extract(day from "root".settlementDateTime) as "Settlement Day Of Month" from tradeTable as "root" where "root".ID = 9',
               lambda f: f.find_all(None, None, [f.settlement_date_time(), f.settlement_date_time().day()], f.id_().eq(9))),
        ported("testYearZeroToOne", f"{_SRC}::testYearZeroToOne",
               'select extract(year from "root".settlementDateTime) as "year" from tradeTable as "root" where "root".ID in (9, 10)',
               lambda f: f.find_all(None, None, [f.settlement_date_time().year()], f.id_().is_in([9, 10]))),
        unsupported("testDateDiffZeroToOne", f"{_SRC}::testDateDiffZeroToOne",
                    'select datediff(day, "root".tradeDate, "root".settlementDateTime) as "daysDiff" from tradeTable as "root" where "root".ID in (9, 10)',
                    'no date difference between two columns'),
    ],
)
