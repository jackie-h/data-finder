"""
Ported from legend-engine core_relational ``tds/tests/testTDSFilter.pure``.

Filters on projected columns, which are filters on the properties here.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTDSFilter.pure"

TDS_FILTER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testFilterTwoExpressions", f"{_SRC}::testFilterTwoExpressions",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().eq('John').and_op(f.last_name().eq('Johnson')))),
        ported("testFilterTwoExpressions_chain", f"{_SRC}::testFilterTwoExpressions_chain",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().eq('John').and_op(f.last_name().eq('Johnson')))),
        ported("testFilterMultipleExpressions1", f"{_SRC}::testFilterMultipleExpressions1",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME is not null and "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().is_not_none().and_op(f.first_name().eq('John'))
                                    .and_op(f.last_name().eq('Johnson')))),
        ported("testFilterMultipleExpressions1_chain", f"{_SRC}::testFilterMultipleExpressions1_chain",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME is not null and "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().is_not_none().and_op(f.first_name().eq('John'))
                                    .and_op(f.last_name().eq('Johnson')))),
        ported("testFilterBeforeAndFilterAfter", f"{_SRC}::testFilterBeforeAndFilterAfter",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".LASTNAME = \'Johnson\' and "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.last_name().eq('Johnson').and_op(f.first_name().eq('John')))),
        ported("testFilterUsingFunction", f"{_SRC}::testFilterUsingFunction",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME like \'A%\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()], f.first_name().starts_with('A'))),
        unsupported("testFilterMultipleExpressions2", f"{_SRC}::testFilterMultipleExpressions2",
                    'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where ("root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\') or ("root".FIRSTNAME = \'NotJohn1\' and "root".LASTNAME = \'NotJohnson1\') or ("root".FIRSTNAME = \'NotJohn2\' and "root".LASTNAME = \'NotJohnson2\')',
                    'no OR operator on filters'),
        unsupported("testFilterBeforeWithOrAndFilterAfter", f"{_SRC}::testFilterBeforeWithOrAndFilterAfter",
                    'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where ("root".LASTNAME = \'Johnson\' or "root".LASTNAME = \'Hill\') and "root".FIRSTNAME = \'John\'',
                    'no OR operator on filters'),
        unsupported("testEvalInFilter", f"{_SRC}::testEvalInFilter",
                    'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".LASTNAME = \'Johnson\' or "root".LASTNAME = \'Hill\'',
                    'no OR operator on filters'),
        unsupported("testFirstNotNullFunction", f"{_SRC}::testFirstNotNullFunction",
                    'select "root".FIRSTNAME as "firstName" from personTable as "root" where coalesce("root".FIRSTNAME, \'N/A\') <> \'N/A\' or coalesce("root".FIRSTNAME, \'N/A\') is null',
                    'no coalesce / firstNotNull'),
        unsupported("testFilterOnDynaFunctionColumn", f"{_SRC}::testFilterOnDynaFunctionColumn",
                    'select "root".FIRSTNAME as "first", "root".LASTNAME like \'J%\' as "J" from personTable as "root" where "root".LASTNAME like \'J%\'',
                    'no boolean expression as a column'),
    ],
)
