"""
Ported from legend-engine core_relational ``tests/mapping/propertyfunc/simplePropertyFunc.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/propertyfunc/simplePropertyFunc.pure"


SIMPLE_PROPERTY_FUNC_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSimplePropertyFuncProject", f"{_SRC}::testSimplePropertyFuncProject",
                    'select substring("root".FULLNAME, 0, locate(\',\', "root".FULLNAME) - 1) as "firstName", substring("root".FULLNAME, locate(\',\', "root".FULLNAME) + 2) as "lastName", concat(substring("root".FULLNAME, locate(\',\', "root".FULLNAME) + 2), \', \', substring("root".FULLNAME, 0, locate(\',\', "root".FULLNAME) - 1)) as "fullName" from personTable as "root"',
                    'a property mapped to a function'),
        unsupported("testSimplePropertyFuncProjectWithJoin", f"{_SRC}::testSimplePropertyFuncProjectWithJoin",
                    'select substring("root".FULLNAME, 0, locate(\',\', concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2)) - 1) as "firstName", substring(concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2), locate(\',\', concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2)) + 2) as "lastName", concat(substring(concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2), locate(\',\', concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2)) + 2), \', \', substring("root".FULLNAME, 0, locate(\',\', concat("root".FULLNAME, "persontableextension_0".FULLNAME_PART2)) - 1)) as "fullName" from personTable as "root" left outer join personTableExtension as "persontableextension_0" on ("root".ID = "persontableextension_0".ID)',
                    'a property mapped to a function'),
    ],
)
