"""
Ported from legend-engine core_relational ``tds/tests/testTdsRenameColumns.pure``.

Renaming output columns.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder import constant, count_all
from datafinder_legend_tests.legend import BACKEND, CHAINED_RESULT_SUBSELECT, ported, unsupported

_SRC = "tds/tests/testTdsRenameColumns.pure"

RENAME_COLUMNS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testRenameColumns", f"{_SRC}::testRenameColumns",
                    'select "persontable_0"."firstName" as "First Name", "persontable_0"."hello" as "Other" from (select "root".FIRSTNAME as "firstName", \'Hello\' as "hello" from personTable as "root") as "persontable_0"',
                    lambda f: f.find_all([f.first_name().alias("firstName"), constant("Hello").alias("hello")]).extend(**{"First Name": lambda r: r.firstName, "Other": lambda r: r.hello}).select("First Name", "Other"),
                    known_gaps={BACKEND: CHAINED_RESULT_SUBSELECT}),
        ported("testRenameColumnsAfterGroupBy", f"{_SRC}::testRenameColumnsAfterGroupBy",
                    'select "persontable_0"."firstName" as "First Name", "persontable_0"."count" as "People Count" from (select "root".FIRSTNAME as "firstName", count(*) as "count" from personTable as "root" group by "firstName" order by "firstName" asc) as "persontable_0"',
                    lambda f: (lambda g: g.order_by(g["firstName"].ascending()).extend(**{"First Name": lambda r: r.firstName, "People Count": lambda r: r.count}).select("First Name", "People Count"))(f.find_all([f.first_name().alias("firstName"), f.age().alias("age")]).group_by(lambda r: r.firstName).aggregate(count=lambda r: count_all())),
                    known_gaps={BACKEND: CHAINED_RESULT_SUBSELECT}),
        ported("testRenameColumnsAfterConcatenate", f"{_SRC}::testRenameColumnsAfterConcatenate",
                    'select "unionalias_0"."firstName" as "First Name" from (select "unionalias_1"."firstName" as "firstName" from (select "root".FIRSTNAME as "firstName" from personTable as "root" union all select "root".FIRSTNAME as "firstName" from personTable as "root") as "unionalias_1") as "unionalias_0"',
                    lambda f: (lambda c: c.extend(**{"First Name": lambda r: r.firstName}).select("First Name"))(f.find_all([f.first_name().alias("firstName")]).concatenate(f.find_all([f.first_name().alias("firstName")]), all=True)),
                    known_gaps={BACKEND: CHAINED_RESULT_SUBSELECT}),
    ],
)
