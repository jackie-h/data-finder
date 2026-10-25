"""
Ported from legend-engine core_relational ``tds/tests/testTDSWindowColumn.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tds/tests/testTDSWindowColumn.pure"


T_D_S_WINDOW_COLUMN_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDistinctBeforeWindowFunction", f"{_SRC}::testDistinctBeforeWindowFunction",
                    'select "subselect"."source" as "source", "subselect"."target" as "target", count("subselect"."target") over (partition by "subselect"."source") as "testCol1" from (select distinct "persontable_0".FIRSTNAME as "source", "persontable_1".FIRSTNAME as "target" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) left outer join personTable as "persontable_1" on ("root".targetId = "persontable_1".ID)) as "subselect"',
                    'a mapping that the port does not load'),
        unsupported("testDistinctAfterWindowFunction", f"{_SRC}::testDistinctAfterWindowFunction",
                    'select distinct "subselect"."source" as "source", "subselect"."target" as "target", "subselect"."testCol1" as "testCol1" from (select "persontable_0".FIRSTNAME as "source", "persontable_1".FIRSTNAME as "target", count("persontable_1".FIRSTNAME) over (partition by "persontable_0".FIRSTNAME) as "testCol1" from interactionTable as "root" left outer join personTable as "persontable_0" on ("root".sourceId = "persontable_0".ID) left outer join personTable as "persontable_1" on ("root".targetId = "persontable_1".ID)) as "subselect"',
                    'a mapping that the port does not load'),
    ],
)
