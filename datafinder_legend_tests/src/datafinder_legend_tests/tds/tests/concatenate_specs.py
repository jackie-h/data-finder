"""
Ported from legend-engine core_relational ``tds/tests/testTDSConcatenate.pure``.

Concatenating (UNION ALL) two projections.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTDSConcatenate.pure"

CONCATENATE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSimpleConcatenate", f"{_SRC}::testSimpleConcatenate",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
                    lambda f: f.find_all([f.last_name().alias("lastName")]).concatenate(f.find_all([f.last_name().alias("lastName")]), all=True)),
        ported("testMultiConcatenate", f"{_SRC}::testMultiConcatenate",
                    'select "unionalias_0"."lastName" as "lastName" from (select concat("root".LASTNAME, \'_0\') as "lastName" from personTable as "root" union all select concat("root".LASTNAME, \'_1\') as "lastName" from personTable as "root" union all select concat("root".LASTNAME, \'_2\') as "lastName" from personTable as "root" union all select concat("root".LASTNAME, \'_3\') as "lastName" from personTable as "root") as "unionalias_0"',
                    lambda f: f.find_all([(f.last_name() + '_0').alias("lastName")]).concatenate(f.find_all([(f.last_name() + '_1').alias("lastName")]), all=True).concatenate(f.find_all([(f.last_name() + '_2').alias("lastName")]), all=True).concatenate(f.find_all([(f.last_name() + '_3').alias("lastName")]), all=True)),
        ported("testConcatenateWithPreOperation1", f"{_SRC}::testConcatenateWithPreOperation1",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" where "root".LASTNAME <> \'hello\' or "root".LASTNAME is null union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
                    lambda f: f.find_all([f.last_name().alias("lastName")], f.last_name() != 'hello').concatenate(f.find_all([f.last_name().alias("lastName")]), all=True)),
        unsupported("testConcatenateWithPostOperation1", f"{_SRC}::testConcatenateWithPostOperation1",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0" where "unionalias_0"."lastName" <> \'hello\' or "unionalias_0"."lastName" is null',
                    'a filter or join on a concatenated result: results cannot be chained yet'),
        unsupported("testConcatenateWithJoin", f"{_SRC}::testConcatenateWithJoin",
                    'select "unionalias_0"."lhs_lastName" as "lhs_lastName", "unionalias_0"."rhs_firstName" as "rhs_firstName" from (select "unionalias_1"."lhs_lastName" as "lhs_lastName", "persontable_1"."rhs_lastName" as "rhs_lastName", "persontable_1"."rhs_firstName" as "rhs_firstName" from (select "unionalias_2"."lhs_lastName" as "lhs_lastName" from (select "root".LASTNAME as "lhs_lastName" from personTable as "root" union all select "root".LASTNAME as "lhs_lastName" from personTable as "root") as "unionalias_2") as "unionalias_1" inner join (select "root".LASTNAME as "rhs_lastName", "root".FIRSTNAME as "rhs_firstName" from personTable as "root") as "persontable_1" on ("unionalias_1"."lhs_lastName" = "persontable_1"."rhs_lastName")) as "unionalias_0"',
                    'a filter or join on a concatenated result: results cannot be chained yet'),
        ported("testConcatenateWithPreOperation2", f"{_SRC}::testConcatenateWithPreOperation2",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root" where "root".LASTNAME <> \'hello\' or "root".LASTNAME is null) as "unionalias_0"',
                    lambda f: f.find_all([f.last_name().alias("lastName")]).concatenate(f.find_all([f.last_name().alias("lastName")], f.last_name() != 'hello'), all=True)),
        ported("testConcatenateWithPreOperation3", f"{_SRC}::testConcatenateWithPreOperation3",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" where "root".LASTNAME = \'hello\' union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0"',
                    lambda f: f.find_all([f.last_name().alias("lastName")], f.last_name().eq('hello')).concatenate(f.find_all([f.last_name().alias("lastName")]), all=True)),
        ported("testConcatenateWithPreOperation4", f"{_SRC}::testConcatenateWithPreOperation4",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root" where "root".LASTNAME = \'hello\') as "unionalias_0"',
                    lambda f: f.find_all([f.last_name().alias("lastName")]).concatenate(f.find_all([f.last_name().alias("lastName")], f.last_name().eq('hello')), all=True)),
        unsupported("testConcatenateWithPostOperation2", f"{_SRC}::testConcatenateWithPostOperation2",
                    'select "unionalias_0"."lastName" as "lastName" from (select "root".LASTNAME as "lastName" from personTable as "root" union all select "root".LASTNAME as "lastName" from personTable as "root") as "unionalias_0" where "unionalias_0"."lastName" = \'hello\'',
                    'a filter or join on a concatenated result: results cannot be chained yet'),
    ],
)
