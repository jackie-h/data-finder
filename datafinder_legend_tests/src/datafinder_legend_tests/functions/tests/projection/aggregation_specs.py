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
        unsupported("testProjectQualifierContainingAggregation", f"{_SRC}::testProjectQualifierContainingAggregation",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "sum" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, sum("persontable_0".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    'no qualified (derived) properties'),
        unsupported("testProjectQualifierContainingAggregationWithSubAgg", f"{_SRC}::testProjectQualifierContainingAggregationWithSubAgg",
                    'select "root".LEGALNAME as "legalName", "firmtable_1".aggCol as "count", "firmtable_3".aggCol as "sum" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, count("persontable_0".LASTNAME) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join (select "firmtable_2".ID as ID, sum("persontable_1".AGE) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_1" on ("firmtable_2".ID = "persontable_1".FIRMID) group by "firmtable_2".ID) as "firmtable_3" on ("root".ID = "firmtable_3".ID)',
                    'no qualified (derived) properties'),
        ported("testDeepSubAggregation", f"{_SRC}::testDeepSubAggregation",
               'select "root".LEGALNAME as "c", "firmtable_1".aggCol as "b", "firmtable_3".aggCol as "a" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, count("addresstable_0".NAME) as aggCol from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_0".ADDRESSID) group by "firmtable_2".ID) as "firmtable_1" on ("root".ID = "firmtable_1".ID) left outer join (select "firmtable_4".ID as ID, avg(1.0 * "persontable_1".AGE) as aggCol from firmTable as "firmtable_4" left outer join personTable as "persontable_1" on ("firmtable_4".ID = "persontable_1".FIRMID) group by "firmtable_4".ID) as "firmtable_3" on ("root".ID = "firmtable_3".ID)',
               lambda f: f.find_all(None, None, [f.legal_name(), f.employees().address().name().count(), f.employees().age().average()]).group_by(f.legal_name()),
               known_gaps={BACKEND: SUB_AGGREGATION_SHAPE}),
        unsupported("testSubAggregationWithDeepAndOverlap_WithColVar", f"{_SRC}::testSubAggregationWithDeepAndOverlap_WithColVar",
                    'select "root".LEGALNAME as "a", "persontable_0".LASTNAME as "b", 2 + "persontable_1".aggCol as "c" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join (select "persontable_2".ID as ID, count("locationtable_0".PLACE) as aggCol from personTable as "persontable_2" left outer join locationTable as "locationtable_0" on ("persontable_2".ID = "locationtable_0".PERSONID) group by "persontable_2".ID) as "persontable_1" on ("persontable_0".ID = "persontable_1".ID)',
                    'no arithmetic on the result of an aggregate'),
        unsupported("testDateAggregationWithMax", f"{_SRC}::testDateAggregationWithMax",
                    'select "root".legalName as "legalName", "firmextensiontable_1".aggCol as "birthDateOfOldestEmployee" from firmExtensionTable as "root" left outer join (select "firmextensiontable_2".firmId as firmId, max("persontableextension_0".birthDate) as aggCol from firmExtensionTable as "firmextensiontable_2" left outer join PersonTableExtension as "persontableextension_0" on ("firmextensiontable_2".firmId = "persontableextension_0".FIRMID) group by "firmextensiontable_2".firmId) as "firmextensiontable_1" on ("root".firmId = "firmextensiontable_1".firmId)',
                    'FirmExtension is not in the model'),
    ],
)
