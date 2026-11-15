"""
Ported from legend-engine core_relational ``tests/advanced/testFilterWithQualifiedProperties.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, QUALIFIED_PROPERTIES, not_supported

_SRC = "tests/advanced/testFilterWithQualifiedProperties.pure"


FILTER_WITH_QUALIFIED_PROPERTIES_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        not_supported("projectFunctionExpressionWithConditionOnRightTable", f"{_SRC}::projectFunctionExpressionWithConditionOnRightTable",
                    'select "persontable_0".LASTNAME as "c1" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'lala\')',
                    QUALIFIED_PROPERTIES),
        not_supported("projectAbstractPropertyWithConditionOnRightTable", f"{_SRC}::projectAbstractPropertyWithConditionOnRightTable",
                    'select "persontable_0".LASTNAME as "c1" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'lala\')',
                    QUALIFIED_PROPERTIES),
        not_supported("projectAbstractPropertyWithConditionOnRightTableNested", f"{_SRC}::projectAbstractPropertyWithConditionOnRightTableNested",
                    'select "persontable_0".LASTNAME as "c1" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) where "addresstable_0".NAME = \'lala\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
                    QUALIFIED_PROPERTIES),
        not_supported("projectFunctionExpressionWithConditionOnRightTableNested", f"{_SRC}::projectFunctionExpressionWithConditionOnRightTableNested",
                    'select "persontable_0".LASTNAME as "c1" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) where "addresstable_0".NAME = \'lala\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID)',
                    QUALIFIED_PROPERTIES),
        not_supported("projectAbstractPropertyWithConditionOnLeftAndRightTable", f"{_SRC}::projectAbstractPropertyWithConditionOnLeftAndRightTable",
                    'select "firmtable_1".LASTNAME as "c1" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".LASTNAME as LASTNAME from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) where concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) = "addresstable_0".NAME) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    QUALIFIED_PROPERTIES),
        not_supported("projectFunctionExpressionWithConditionOnLeftAndRightTableNestedDynaFunction", f"{_SRC}::projectFunctionExpressionWithConditionOnLeftAndRightTableNestedDynaFunction",
                    'select "firmtable_1".FIRSTNAME as "c1" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".FIRSTNAME as FIRSTNAME from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) where concat("persontable_0".FIRSTNAME, \' \', "persontable_0".LASTNAME) = "addresstable_0".NAME) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    QUALIFIED_PROPERTIES),
        not_supported("projectFunctionExpressionWithConditionOnLeftAndRightTableNested", f"{_SRC}::projectFunctionExpressionWithConditionOnLeftAndRightTableNested",
                    'select "firmtable_1".LASTNAME as "c1" from firmTable as "root" left outer join (select "firmtable_2".ID as ID, "persontable_0".LASTNAME as LASTNAME from firmTable as "firmtable_2" left outer join personTable as "persontable_0" on ("firmtable_2".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID) where "persontable_0".LASTNAME is not distinct from "addresstable_0".NAME) as "firmtable_1" on ("root".ID = "firmtable_1".ID)',
                    QUALIFIED_PROPERTIES),
        not_supported("projectFunctionExpressionWithConditionOnLeftAndRightTable", f"{_SRC}::projectFunctionExpressionWithConditionOnLeftAndRightTable",
                    'select "persontable_0".LASTNAME as "c1" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = "root".LEGALNAME)',
                    QUALIFIED_PROPERTIES),
        not_supported("filterAndProjectFunctionExpressionWithConditionOnRightTable", f"{_SRC}::filterAndProjectFunctionExpressionWithConditionOnRightTable",
                    'select "persontable_0".FIRSTNAME as "c1" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') where "persontable_0".FIRSTNAME = \'Peter\'',
                    QUALIFIED_PROPERTIES),
        not_supported("nestedFilterFunctionExpressionWithOrCondition", f"{_SRC}::nestedFilterFunctionExpressionWithOrCondition",
                    'select "root".LEGALNAME as "Firm Name" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') left outer join personTable as "persontable_1" on ("root".ID = "persontable_1".FIRMID and "persontable_1".LASTNAME = \'Smith\') where ("persontable_0".AGE is not null and "persontable_1".AGE > 20) or "root".LEGALNAME = \'Firm B\'',
                    QUALIFIED_PROPERTIES),
        not_supported("nestedFilterInQualifierWithOrCondition", f"{_SRC}::nestedFilterInQualifierWithOrCondition",
                    'select "root".LEGALNAME as "Firm Name" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') left outer join personTable as "persontable_1" on ("root".ID = "persontable_1".FIRMID and "persontable_1".LASTNAME = \'Smith\') where ("persontable_0".AGE is not null and "persontable_1".AGE > 20) or "root".LEGALNAME = \'Firm B\'',
                    QUALIFIED_PROPERTIES),
        not_supported("projectionInvolvingOneNestedAtributeOnTriangularJoinAndOtherJoin", f"{_SRC}::projectionInvolvingOneNestedAtributeOnTriangularJoinAndOtherJoin",
                    'select "root".LEGALNAME as "c1", "persontable_0".LASTNAME as "c2", concat("persontable_0".LASTNAME, "addresstable_0".NAME) as "c3" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "locationtable_0".PLACE as PLACE, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join locationTable as "locationtable_0" on ("persontable_1".ID = "locationtable_0".PERSONID)) as "persontable_0" on ("persontable_0".PLACE = "root".LEGALNAME and "root".ID = "persontable_0".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID)',
                    QUALIFIED_PROPERTIES),
    ],
)
