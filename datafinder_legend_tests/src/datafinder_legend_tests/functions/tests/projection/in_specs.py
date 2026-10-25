"""
Ported from legend-engine core_relational ``functions/tests/projection/testIn.pure``.

Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testIn.pure"

_IN_JOHN_PETER = (
    'select "root".LASTNAME as "lastName" from personTable as "root" '
    "where \"root\".FIRSTNAME in ('John', 'Peter')"
)
_DEEP_IN = (
    'select "root".LASTNAME as "lastName" from personTable as "root" '
    'left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) '
    'left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) '
    "where \"addresstable_0\".NAME in ('New York', 'Tokyo')"
)
_NOT_IN = (
    'select "root".LASTNAME as "lastName" from personTable as "root" '
    "where not \"root\".FIRSTNAME in ('John', 'Peter') or \"root\".FIRSTNAME is null"
)

IN_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testIn", f"{_SRC}::testIn", _IN_JOHN_PETER,
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        ported("testContains", f"{_SRC}::testContains", _IN_JOHN_PETER,
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        ported("testDeepIn", f"{_SRC}::testDeepIn", _DEEP_IN,
               lambda f: f.find_all(None, None, [f.last_name()], f.firm().address().name().is_in(["New York", "Tokyo"]))),
        ported("testDeepContains", f"{_SRC}::testDeepContains", _DEEP_IN,
               lambda f: f.find_all(None, None, [f.last_name()], f.firm().address().name().is_in(["New York", "Tokyo"]))),
        ported("testNotIn", f"{_SRC}::testNotIn", _NOT_IN,
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_not_in(["John", "Peter"]))),
        ported("testNotContains", f"{_SRC}::testNotContains", _NOT_IN,
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_not_in(["John", "Peter"]))),
        ported("testDeepInWithMultipleProject", f"{_SRC}::testDeepInWithMultipleProject",
               'select "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) where "addresstable_0".NAME in (\'New York\', \'Tokyo\')',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name()], f.firm().address().name().is_in(["New York", "Tokyo"]))),
        ported("testDeepContainsWithMultipleProject", f"{_SRC}::testDeepContainsWithMultipleProject",
               'select "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) where "addresstable_0".NAME in (\'New York\', \'Tokyo\')',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name()], f.firm().address().name().is_in(["New York", "Tokyo"]))),
        ported("testInWithOneValue", f"{_SRC}::testInWithOneValue",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John"]))),
        ported("testContainsWithOneValue", f"{_SRC}::testContainsWithOneValue",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John"]))),
        ported("testInWithVariables", f"{_SRC}::testInWithVariables",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME in (\'John\', \'Peter\')',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        ported("testContainsWithVariables", f"{_SRC}::testContainsWithVariables",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME in (\'John\', \'Peter\')',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        ported("testInWithListVariable", f"{_SRC}::testInWithListVariable",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME in (\'John\', \'Peter\')',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        ported("testContainsWithListVariable", f"{_SRC}::testContainsWithListVariable",
               'select "root".LASTNAME as "lastName" from personTable as "root" where "root".FIRSTNAME in (\'John\', \'Peter\')',
               lambda f: f.find_all(None, None, [f.last_name()], f.first_name().is_in(["John", "Peter"]))),
        unsupported("testInWithDynaFunction", f"{_SRC}::testInWithDynaFunction",
                    'select "root".ID as "id", case when "root".active = \'Y\' then \'true\' else \'false\' end as "active" from interactionTable as "root" where case when "root".active = \'Y\' then \'true\' else \'false\' end in (\'false\', \'something\') and "root".ID = 4',
                    'Interaction is not in the model, and its active flag is a derived property'),
        unsupported("testQualifierWithInThroughJoin", f"{_SRC}::testQualifierWithInThroughJoin",
                    'select "accounttable_0".name as "name", case when "accounttable_0".name in (\'Account 1\', \'Account 2\') then \'A\' else \'B\' end as "cat" from tradeTable as "root" left outer join accountTable as "accounttable_0" on ("root".accountID = "accounttable_0".ID) where "root".ID = 10 or "root".ID = 11',
                    'no qualified (derived) properties'),
    ],
)
