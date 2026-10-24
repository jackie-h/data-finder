"""
Ported from legend-engine core_relational ``tds/tests/testTDSExtend.pure``.

Adding columns to a projection.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTDSExtend.pure"

EXTEND_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testStringColumn", f"{_SRC}::testStringColumn",
               'select "root".FIRSTNAME as "first_name", "root".FIRSTNAME as "name_again" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.first_name()])),
        ported("testFunctionOnStringColumn", f"{_SRC}::testFunctionOnStringColumn",
               'select "root".FIRSTNAME as "first_name", char_length("root".FIRSTNAME) as "name_length" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.first_name().length()])),
        unsupported("testStringLiteral", f"{_SRC}::testStringLiteral",
                    'select "root".FIRSTNAME as "firstName", \'Hello\' as "hello" from personTable as "root"',
                    'no constant columns'),
        unsupported("testFunctionOnStringLiteral", f"{_SRC}::testFunctionOnStringLiteral",
                    'select "root".FIRSTNAME as "first_name", char_length(\'Hello\') as "hello_length" from personTable as "root"',
                    'no constant columns'),
        unsupported("testMultipleFunctions", f"{_SRC}::testMultipleFunctions",
                    'select "root".FIRSTNAME as "first_name", \'Hello\' as "const", char_length("root".FIRSTNAME) as "name_length" from personTable as "root"',
                    'no constant columns'),
        unsupported("testExtendAndExtendAgain", f"{_SRC}::testExtendAndExtendAgain",
                    'select "root".FIRSTNAME as "first_name", char_length("root".FIRSTNAME) as "name_length", char_length("root".FIRSTNAME) * 2 as "double_name_length" from personTable as "root"',
                    'no arithmetic on columns'),
        unsupported("testExtendOnDynaFunctionColumn", f"{_SRC}::testExtendOnDynaFunctionColumn",
                    'select "root".FIRSTNAME as "first", upper("root".LASTNAME) as "LAST", char_length(upper("root".LASTNAME)) as "name_length" from personTable as "root"',
                    'scalar functions cannot be chained (upper(...).length())'),
        unsupported("testExtendOnMultiple", f"{_SRC}::testExtendOnMultiple",
                    'select "root".FIRSTNAME as "first_name", "root".LASTNAME as "last_name", char_length("root".FIRSTNAME) + char_length("root".LASTNAME) as "combined_length" from personTable as "root"',
                    'no arithmetic between columns'),
        unsupported("testDateLiteral", f"{_SRC}::testDateLiteral",
                    'select "root".FIRSTNAME as "firstName", \'2016-08-10\' as "dateLiteral" from personTable as "root"',
                    'no constant columns'),
        unsupported("testNoopFunctions", f"{_SRC}::testNoopFunctions",
                    'select "root".FIRSTNAME as "firstName", "root".FIRSTNAME as "toOne", "root".FIRSTNAME as "first", null as "castNull", "root".FIRSTNAME as "cast" from personTable as "root"',
                    'no constant columns'),
        unsupported("testFirstNotNullFunction", f"{_SRC}::testFirstNotNullFunction",
                    'select "root".FIRSTNAME as "firstName", coalesce("root".FIRSTNAME, \'N/A\') as "first" from personTable as "root"',
                    'no coalesce / firstNotNull'),
        unsupported("testNull", f"{_SRC}::testNull",
                    'select "root".FIRSTNAME as "firstName", "root".FIRSTNAME is null as "isNull", "root".FIRSTNAME is not null as "isNotNull" from personTable as "root"',
                    'no boolean expression as a column'),
        unsupported("testIfWithEmptyExtension", f"{_SRC}::testIfWithEmptyExtension",
                    'select "root".FIRSTNAME as "firstName", case when "root".FIRSTNAME is not null then "root".FIRSTNAME else null end as "extended" from personTable as "root"',
                    'no conditional expression'),
    ],
)
