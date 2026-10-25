"""
Ported from legend-engine core_relational ``tds/tests/testTDSRestrictDistinct.pure``.

Distinct on a restricted set of columns.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, JOIN_OPERAND_ORDER

_SRC = "tds/tests/testTDSRestrictDistinct.pure"

RESTRICT_DISTINCT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testRestrictDistinct_JoinCutDown_ProjectFunctions", f"{_SRC}::testRestrictDistinct_JoinCutDown_ProjectFunctions",
               'select distinct "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()]).distinct()),
        ported("testRestrictDistinct_JoinCutDown_ColumnSpecs", f"{_SRC}::testRestrictDistinct_JoinCutDown_ColumnSpecs",
               'select distinct "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()]).distinct()),
        unsupported("testRestrictDistinct_NoOptimization_NotAfterProject", f"{_SRC}::testRestrictDistinct_NoOptimization_NotAfterProject",
                    'select distinct "subselect"."firstName" as "firstName" from (select top 1000 "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "firmName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)) as "subselect"',
                    'restrict, sort, limit and distinct in sequence nest subselects, and Legend keeps joins to restricted-away columns; the finder builds one query'),
        unsupported("testRestrictDistinct_NoOptimization_NotRestrictFollowedByDistinct", f"{_SRC}::testRestrictDistinct_NoOptimization_NotRestrictFollowedByDistinct",
                    'select distinct "root".FIRSTNAME as "firstName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) order by "firstName" asc',
                    'restrict, sort, limit and distinct in sequence nest subselects, and Legend keeps joins to restricted-away columns; the finder builds one query'),
        ported("testRestrictDistinct_NoJoinCutDown", f"{_SRC}::testRestrictDistinct_NoJoinCutDown",
               'select distinct "firmtable_0".LEGALNAME as "firmName", "root".LASTNAME as "lastName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)',
               lambda f: f.find_all(None, None, [f.firm().legal_name(), f.last_name()]).distinct()),
        ported("testRestrictDistinct_ColumnOrdering", f"{_SRC}::testRestrictDistinct_ColumnOrdering",
               'select distinct "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.last_name(), f.first_name()]).distinct()),
        unsupported("testRestrictDistinct_JoinCutDown_PostTdsOperations", f"{_SRC}::testRestrictDistinct_JoinCutDown_PostTdsOperations",
                    'select count(*) as "cnt" from (select "subselect"."firstName" as "firstName" from (select top 1000 distinct "root".FIRSTNAME as "firstName" from personTable as "root") as "subselect" order by "firstName" asc) as "persontable_0"',
                    'restrict, distinct, take, sort and group by in sequence nest subselects'),
    ],
)
