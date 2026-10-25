"""
Ported from legend-engine core_relational ``tds/tests/testTDSFilter.pure``.

Filters on projected columns, which are filters on the properties here.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, CONCAT_FUNCTION

_SRC = "tds/tests/testTDSFilter.pure"

TDS_FILTER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testFilterTwoExpressions", f"{_SRC}::testFilterTwoExpressions",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().eq('John').and_(f.last_name().eq('Johnson')))),
        ported("testFilterTwoExpressions_chain", f"{_SRC}::testFilterTwoExpressions_chain",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().eq('John').and_(f.last_name().eq('Johnson')))),
        ported("testFilterMultipleExpressions1", f"{_SRC}::testFilterMultipleExpressions1",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME is not null and "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().is_not_none().and_(f.first_name().eq('John'))
                                    .and_(f.last_name().eq('Johnson')))),
        ported("testFilterMultipleExpressions1_chain", f"{_SRC}::testFilterMultipleExpressions1_chain",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME is not null and "root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().is_not_none().and_(f.first_name().eq('John'))
                                    .and_(f.last_name().eq('Johnson')))),
        ported("testFilterBeforeAndFilterAfter", f"{_SRC}::testFilterBeforeAndFilterAfter",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".LASTNAME = \'Johnson\' and "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.last_name().eq('Johnson').and_(f.first_name().eq('John')))),
        ported("testFilterUsingFunction", f"{_SRC}::testFilterUsingFunction",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".FIRSTNAME like \'A%\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()], f.first_name().starts_with('A'))),
        ported("testFilterMultipleExpressions2", f"{_SRC}::testFilterMultipleExpressions2",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where ("root".FIRSTNAME = \'John\' and "root".LASTNAME = \'Johnson\') or ("root".FIRSTNAME = \'NotJohn1\' and "root".LASTNAME = \'NotJohnson1\') or ("root".FIRSTNAME = \'NotJohn2\' and "root".LASTNAME = \'NotJohnson2\')',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.first_name().eq('John').and_(f.last_name().eq('Johnson'))
                                    .or_(f.first_name().eq('NotJohn1').and_(f.last_name().eq('NotJohnson1')))
                                    .or_(f.first_name().eq('NotJohn2').and_(f.last_name().eq('NotJohnson2'))))),
        ported("testFilterBeforeWithOrAndFilterAfter", f"{_SRC}::testFilterBeforeWithOrAndFilterAfter",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where ("root".LASTNAME = \'Johnson\' or "root".LASTNAME = \'Hill\') and "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.last_name().eq('Johnson').or_(f.last_name().eq('Hill'))
                                    .and_(f.first_name().eq('John')))),
        ported("testEvalInFilter", f"{_SRC}::testEvalInFilter",
               'select "root".FIRSTNAME as "first", "root".LASTNAME as "last" from personTable as "root" where "root".LASTNAME = \'Johnson\' or "root".LASTNAME = \'Hill\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name()],
                                    f.last_name().eq('Johnson').or_(f.last_name().eq('Hill')))),
        unsupported("testFirstNotNullFunction", f"{_SRC}::testFirstNotNullFunction",
                    'select "root".FIRSTNAME as "firstName" from personTable as "root" where coalesce("root".FIRSTNAME, \'N/A\') <> \'N/A\' or coalesce("root".FIRSTNAME, \'N/A\') is null',
                    "Legend writes `!=` as `<> x or expr is null`; the finder's `!=` is just `<> x`"),
        ported("testFilterOnDynaFunctionColumn", f"{_SRC}::testFilterOnDynaFunctionColumn",
               'select "root".FIRSTNAME as "first", "root".LASTNAME like \'J%\' as "J" from personTable as "root" where "root".LASTNAME like \'J%\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.last_name().starts_with("J")], f.last_name().starts_with("J"))),
        ported("testSimpleFilter", f"{_SRC}::testSimpleFilter",
               'select "root".FIRSTNAME as "name" from personTable as "root" where "root".FIRSTNAME = \'John\'',
               lambda f: f.find_all(None, None, [f.first_name()], f.first_name() == "John")),
        ported("testFilterWithStringConcat", f"{_SRC}::testFilterWithStringConcat",
               'select "root".FIRSTNAME as "name" from personTable as "root" where concat("root".FIRSTNAME, \'A\') = \'JohnA\'',
               lambda f: f.find_all(None, None, [f.first_name()], (f.first_name() + "A") == "JohnA"),
               known_gaps={BACKEND: CONCAT_FUNCTION}),
        unsupported("testFilterOnEnum", f"{_SRC}::testFilterOnEnum",
                    'select "root".NAME as "name", "root".TYPE as "type" from addressTable as "root" where "root".TYPE = 1',
                    'no enumeration properties'),
        unsupported("testFilterOnDecimal", f"{_SRC}::testFilterOnDecimal",
                    'select 1.0 as "decimal" from personTable as "root" where 1.0 = 2.0',
                    'no filter on a constant'),
        unsupported("testFilterOnRootClassAggValueAfterGroupBy", f"{_SRC}::testFilterOnRootClassAggValueAfterGroupBy",
                    'select "root".AGE as "age", listagg("root".FIRSTNAME, \',\') as "firstNamesWithSameAge" from personTable as "root" group by "age" having listagg("root".FIRSTNAME, \',\') like \'%John%\'',
                    'no having (a filter on an aggregate)'),
        unsupported("testFilterOnAssociatedClassAggValueAfterGroupBy", f"{_SRC}::testFilterOnAssociatedClassAggValueAfterGroupBy",
                    'select "root".AGE as "age", listagg("addresstable_0".NAME, \',\') as "addresses" from personTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) group by "age" having listagg("addresstable_0".NAME, \',\') like \'%Hoboken%\'',
                    'no having (a filter on an aggregate)'),
        unsupported("testFilterFollowedByGroupByWithFilterOnRootClass", f"{_SRC}::testFilterFollowedByGroupByWithFilterOnRootClass",
                    'select "root".AGE as "age", listagg("root".FIRSTNAME, \',\') as "firstNamesWithSameAge" from personTable as "root" where "root".FIRSTNAME = \'John\' group by "age" having listagg("root".FIRSTNAME, \',\') like \'%John%\'',
                    'no having (a filter on an aggregate)'),
        unsupported("testFilterOnRootClassAggValueAfterGroupByWithHavingMultipleFilters", f"{_SRC}::testFilterOnRootClassAggValueAfterGroupByWithHavingMultipleFilters",
                    'select "root".AGE as "age", listagg("root".FIRSTNAME, \',\') as "firstNamesWithSameAge" from personTable as "root" group by "age" having "root".AGE > 20 and listagg("root".FIRSTNAME, \',\') like \'%John%\'',
                    'no having (a filter on an aggregate)'),
        unsupported("testInOnColumnInSubselect", f"{_SRC}::testInOnColumnInSubselect",
                    'select "subselect"."name" as "name", "subselect"."age" as "age", "subselect"."rank" as "rank" from (select "root".FIRSTNAME as "name", "root".AGE as "age", rank() over (partition by "root".FIRSTNAME order by "root".AGE desc nulls first) as "rank" from personTable as "root") as "subselect" where "subselect"."name" in (\'John\', \'Peter\')',
                    'a filter on a window function column is nested in a subselect'),
    ],
)
