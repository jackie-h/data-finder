"""
Ported from legend-engine core_relational ``tds/tests/testTdsRenameColumns.pure``.

Renaming output columns.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTdsRenameColumns.pure"

RENAME_COLUMNS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testRenameColumns", f"{_SRC}::testRenameColumns",
                    'select "persontable_0"."firstName" as "First Name", "persontable_0"."hello" as "Other" from (select "root".FIRSTNAME as "firstName", \'Hello\' as "hello" from personTable as "root") as "persontable_0"',
                    'output columns cannot be renamed'),
        unsupported("testRenameColumnsAfterGroupBy", f"{_SRC}::testRenameColumnsAfterGroupBy",
                    'select "persontable_0"."firstName" as "First Name", "persontable_0"."count" as "People Count" from (select "root".FIRSTNAME as "firstName", count(*) as "count" from personTable as "root" group by "firstName" order by "firstName" asc) as "persontable_0"',
                    'output columns cannot be renamed'),
        unsupported("testRenameColumnsAfterConcatenate", f"{_SRC}::testRenameColumnsAfterConcatenate",
                    'select "unionalias_0"."firstName" as "First Name" from (select "unionalias_1"."firstName" as "firstName" from (select "root".FIRSTNAME as "firstName" from personTable as "root" union all select "root".FIRSTNAME as "firstName" from personTable as "root") as "unionalias_1") as "unionalias_0"',
                    'output columns cannot be renamed'),
    ],
)
