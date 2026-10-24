"""
Ported from legend-engine core_relational ``functions/tests/projection/testAggregation.pure``.

Sub-aggregations over a to-many association: one row per firm, with the aggregate of its employees. Legend
computes these per root object; here the aggregate is grouped by the root explicitly.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, SUB_AGGREGATION_SHAPE

_SRC = "functions/tests/projection/testAggregation.pure"

SUB_AGGREGATION_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testSubAggregation", f"{_SRC}::testSubAggregation",
               'select "firmtable_1".aggCol as "a" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, avg(1.0 * "persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
               lambda f: f.find_all(None, None, [f.employees().age().average()]).group_by(f.id_()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        ported("testSubAggregationWithAnotherSimpleProperty", f"{_SRC}::testSubAggregationWithAnotherSimpleProperty",
               'select "root".LEGALNAME as "b", "firmtable_1".aggCol as "a" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, avg(1.0 * "persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().age().average()]).group_by(f.legal_name()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        ported("testTwoSubAggregationSameProperty", f"{_SRC}::testTwoSubAggregationSameProperty",
               'select "firmtable_1".aggCol as "b", "firmtable_3".aggCol as "a" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, count("persontable_0".LASTNAME) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join (select "firmtable_2".ID as ID, avg(1.0 * "persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_3" on ("root".ID = "firmtable_3".ID)',
               lambda f: f.find_all(None, None, [f.employees().last_name().count(), f.employees().age().average()]).group_by(f.id_()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        ported("testSubAggregationWithMax", f"{_SRC}::testSubAggregationWithMax",
               'select "root".LEGALNAME as "firmName", "firmtable_1".aggCol as "ageOfOldestEmployee" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, max("persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().age().max()]).group_by(f.legal_name()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        ported("testSubAggregationWithDeep", f"{_SRC}::testSubAggregationWithDeep",
               'select "root".LEGALNAME as "a", "firmtable_1".aggCol as "b" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, count("locationtable_0".PLACE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join locationTable as "locationtable_0" on ("persontable_0".ID = "locationtable_0".PERSONID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().locations().place().count()]).group_by(f.legal_name()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        unsupported("testSubAggregationWithJoinStrings", f"{_SRC}::testSubAggregationWithJoinStrings",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeesFirstNamePerFirm" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg("persontable_0".FIRSTNAME, \'^\') as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'join over a to-many association is computed in a derived table joined back to the root'),
        unsupported("testSubAggregationWithJoinStringsOrderBy", f"{_SRC}::testSubAggregationWithJoinStringsOrderBy",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "employeesFirstNamePerFirm" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, listagg("firmtable_2".FIRSTNAME, \'^\') as aggCol from (select "firmtable_3".ID, "persontable_0".FIRSTNAME from firmTable as "firmtable_3" left outer join personTable as "persontable_0" on ("firmtable_3".ID = "persontable_0".FIRMID) order by "persontable_0".LASTNAME asc) as "firmtable_2" group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'join over a to-many association is computed in a derived table joined back to the root'),
        unsupported("testSubAggregationWithIf", f"{_SRC}::testSubAggregationWithIf",
                    'select "root".LEGALNAME as "a", "firmtable_1".aggCol as "b" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, avg(1.0 * case when "persontable_0".LASTNAME = \'Hill\' then ((1.0 * "persontable_0".AGE) / 2) else 1.0 end) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'no conditional inside the argument of an aggregate'),
        unsupported("testSubAggregationWithTopLevelIf", f"{_SRC}::testSubAggregationWithTopLevelIf",
                    'select case when "root".LEGALNAME = \'Firm X\' then 1 else "firmtable_1".aggCol end as "a" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, avg(1.0 * "persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'a conditional over an aggregate of a to-many association is computed in a derived table'),
        unsupported("testIsDistinctSubAggregation", f"{_SRC}::testIsDistinctSubAggregation",
                    'select "root".LEGALNAME as "LegalName", "firmtable_1".aggCol as "IsDistinctFirstName", "firmtable_3".aggCol as "IsDistinctLastName" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, count(distinct("persontable_0".FIRSTNAME)) = count("persontable_0".FIRSTNAME) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join (select "firmtable_2".ID as ID, count(distinct("persontable_0".LASTNAME)) = count("persontable_0".LASTNAME) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_3" on ("root".ID = "firmtable_3".ID) order by "LegalName" desc',
                    'no isDistinct aggregation'),
    ],
)
