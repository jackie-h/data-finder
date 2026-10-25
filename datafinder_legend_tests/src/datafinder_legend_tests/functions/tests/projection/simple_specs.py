"""
Ported from legend-engine core_relational ``functions/tests/projection/testSimple.pure``.

Mapping: tests/simple_mapping.md (Legend ``simpleRelationalMapping``, Person/Firm/Address/Location subset).
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/projection/testSimple.pure"
_MAPPING = "tests/simple_mapping.md"

_FIRST_NAME = 'select "root".FIRSTNAME as "firstName" from personTable as "root"'
_FIRST_LAST = 'select "root".FIRSTNAME as "firstName", "root".LASTNAME as "lastName" from personTable as "root"'

SIMPLE_PROJECTION_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=_MAPPING,
    expectations=[
        ported("testAllOneSimpleProperty", f"{_SRC}::testAllOneSimpleProperty", _FIRST_NAME,
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testAllOneSimplePropertyWithColsFromFunction",
               f"{_SRC}::testAllOneSimplePropertyWithColsFromFunction", _FIRST_LAST,
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()])),
        ported("testAllOneSimplePropertyUsingVariable", f"{_SRC}::testAllOneSimplePropertyUsingVariable", _FIRST_NAME,
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testAllOneSimplePropertyUsingVariables", f"{_SRC}::testAllOneSimplePropertyUsingVariables",
               'select "root".FIRSTNAME as "First Name" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testAllOneSimplePropertyUsingOpenVariables",
               f"{_SRC}::testAllOneSimplePropertyUsingOpenVariables", _FIRST_NAME,
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testAllTwoSimpleProperties", f"{_SRC}::testAllTwoSimpleProperties", _FIRST_LAST,
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()])),
        unsupported("testAllOneSimplePropertyUsingVariables2", f"{_SRC}::testAllOneSimplePropertyUsingVariables2",
                    'select "root".FIRSTNAME as "First Name" from personTable as "root"',
                    'a mapping that the port does not load'),
    ],
)
