"""
Ported from legend-engine core_relational ``functions/tests/projection/testIn.pure``.

The finders have no `in` operator yet, so every Person test here is recorded as unsupported with the SQL Legend
generates. Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import unsupported

_SRC = "functions/tests/projection/testIn.pure"
_NO_IN = "no `in` / `not in` operator on attributes"

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
        unsupported("testIn", f"{_SRC}::testIn", _IN_JOHN_PETER, _NO_IN),
        unsupported("testContains", f"{_SRC}::testContains", _IN_JOHN_PETER, _NO_IN),
        unsupported("testDeepIn", f"{_SRC}::testDeepIn", _DEEP_IN, _NO_IN),
        unsupported("testDeepContains", f"{_SRC}::testDeepContains", _DEEP_IN, _NO_IN),
        unsupported("testNotIn", f"{_SRC}::testNotIn", _NOT_IN, _NO_IN),
        unsupported("testNotContains", f"{_SRC}::testNotContains", _NOT_IN, _NO_IN),
    ],
)
