"""
Ported from legend-engine core_relational ``tds/tests/testTDSRestrictDistinct.pure``.

Distinct on a restricted set of columns.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import BACKEND, CHAINED_RESULT_SUBSELECT, JOIN_KEPT_AFTER_RESTRICT, ported, unsupported

_SRC = "tds/tests/testTDSRestrictDistinct.pure"

RESTRICT_DISTINCT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testRestrictDistinct_JoinCutDown_ProjectFunctions", f"{_SRC}::testRestrictDistinct_JoinCutDown_ProjectFunctions",
               'select distinct "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all([f.first_name()]).distinct()),
        ported("testRestrictDistinct_JoinCutDown_ColumnSpecs", f"{_SRC}::testRestrictDistinct_JoinCutDown_ColumnSpecs",
               'select distinct "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all([f.first_name()]).distinct()),
        ported("testRestrictDistinct_NoOptimization_NotAfterProject", f"{_SRC}::testRestrictDistinct_NoOptimization_NotAfterProject",
                    'select distinct "subselect"."firstName" as "firstName" from (select top 1000 "root".FIRSTNAME as "firstName", "firmtable_0".LEGALNAME as "firmName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)) as "subselect"',
                    lambda f: f.find_all([f.first_name().alias("firstName"), f.firm().legal_name().alias("firmName")]).limit(1000).select("firstName").distinct()),
        ported("testRestrictDistinct_NoOptimization_NotRestrictFollowedByDistinct", f"{_SRC}::testRestrictDistinct_NoOptimization_NotRestrictFollowedByDistinct",
                    'select distinct "root".FIRSTNAME as "firstName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) order by "firstName" asc',
                    lambda f: (lambda r: (lambda d: d.order_by(d["firstName"].ascending()))(r.distinct()))(f.find_all([f.first_name().alias("firstName"), f.firm().legal_name().alias("firmName")]).select("firstName")),
               known_gaps={BACKEND: JOIN_KEPT_AFTER_RESTRICT}),
        ported("testRestrictDistinct_NoJoinCutDown", f"{_SRC}::testRestrictDistinct_NoJoinCutDown",
               'select distinct "firmtable_0".LEGALNAME as "firmName", "root".LASTNAME as "lastName" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)',
               lambda f: f.find_all([f.firm().legal_name(), f.last_name()]).distinct()),
        ported("testRestrictDistinct_ColumnOrdering", f"{_SRC}::testRestrictDistinct_ColumnOrdering",
               'select distinct "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all([f.last_name(), f.first_name()]).distinct()),
        ported("testRestrictDistinct_JoinCutDown_PostTdsOperations", f"{_SRC}::testRestrictDistinct_JoinCutDown_PostTdsOperations",
                    'select count(*) as "cnt" from (select "subselect"."firstName" as "firstName" from (select top 1000 distinct "root".FIRSTNAME as "firstName" from personTable as "root") as "subselect" order by "firstName" asc) as "persontable_0"',
                    lambda f: (lambda r: r.order_by(r["firstName"].ascending()).aggregate(cnt=lambda r: r.firstName.count()))(
                        f.find_all([f.first_name().alias("firstName"), f.firm().legal_name().alias("firmName")]).select("firstName").distinct().limit(1000)),
                    known_gaps={BACKEND: JOIN_KEPT_AFTER_RESTRICT}),
    ],
)
