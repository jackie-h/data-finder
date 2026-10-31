"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testEmbeddedMapping.pure``.

Properties embedded in a denormalised table with nothing to fall back to.
Mapping: tests/mapping/embedded/denorm_mapping.md.
"""
from datafinder import when
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testEmbeddedMapping.pure"

EMBEDDED_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/embedded_mapping.md",
    expectations=[
        unsupported("testDenormMappingOneToManyProjectLambdaSyntaxWithMap", f"{_SRC}::testDenormMappingOneToManyProjectLambdaSyntaxWithMap",
                    'select "person_firm_denorm_2".PERSON_ADDRESS_NAME as "address" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1") as "root" left outer join PERSON_FIRM_DENORM as "person_firm_denorm_2" on ("root".FIRM_ID = "person_firm_denorm_2".FIRM_ID) order by "address" asc',
                    "a mapping with a distinct firm view of the person table (the mapping's `~distinct`)"),
        unsupported("testDenormMappingOneToManyProjectWithEnum", f"{_SRC}::testDenormMappingOneToManyProjectWithEnum",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root"',
                    'enumeration mappings (`type` maps integer codes to an enumeration)'),
        unsupported("testDenormMappingOneToManyProjectWithFilterOnEnumLeft", f"{_SRC}::testDenormMappingOneToManyProjectWithFilterOnEnumLeft",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root" where "root".PERSON_ADDRESS_TYPE = 1',
                    'enumeration mappings (`type` maps integer codes to an enumeration)'),
        unsupported("testDenormMappingOneToManyProjectWithFilterOnEnumRight", f"{_SRC}::testDenormMappingOneToManyProjectWithFilterOnEnumRight",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root" where "root".PERSON_ADDRESS_TYPE = 1',
                    'enumeration mappings (`type` maps integer codes to an enumeration)'),
        unsupported("testProjectToEmbedded", f"{_SRC}::testProjectToEmbedded",
                    'select "root".FIRM_LEGALNAME as "legalName", "person_firm_denorm_2".PERSON_ADDRESS_NAME as "address" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1") as "root" left outer join PERSON_FIRM_DENORM as "person_firm_denorm_2" on ("root".FIRM_ID = "person_firm_denorm_2".FIRM_ID) order by "legalName" asc, "address" asc',
                    "a mapping with a distinct firm view of the person table (the mapping's `~distinct`)"),
        unsupported("testDenormMappingOneToManyProjectEmbeddedQualifier", f"{_SRC}::testDenormMappingOneToManyProjectEmbeddedQualifier",
                    'select "root".PERSON_FIRSTNAME as "firstName", concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) as "nameAndAddress" from PERSON_FIRM_DENORM as "root"',
                    'qualified (derived) properties'),
        unsupported("testDenormMappingWithQualifierWithIfAndEquals", f"{_SRC}::testDenormMappingWithQualifierWithIfAndEquals",
                    'select "root".PERSON_FIRSTNAME as "name", case when "root".FIRM_LEGALNAME = \'Firm X\' then \'Yes\' else \'No\' end as "FirmX" from PERSON_FIRM_DENORM as "root"',
                    'qualified (derived) properties'),
        unsupported("testFilterWithEmbeddedQualifier", f"{_SRC}::testFilterWithEmbeddedQualifier",
                    'select "root".PERSON_FIRSTNAME as "firstName", concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) as "nameAndAddress" from PERSON_FIRM_DENORM as "root" where concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) is not null and concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) like \'Firm X%\'',
                    'qualified (derived) properties'),
        unsupported("testMapEmbeddedQualifierWithIfTwoEmbeddedProperties", f"{_SRC}::testMapEmbeddedQualifierWithIfTwoEmbeddedProperties",
                    'select case when "root".FIRM_LEGALNAME = \'Firm X\' then concat("root".FIRM_LEGALNAME, \' , Top Secret\') else concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) end as "nameAndMaskedAddress" from PERSON_FIRM_DENORM as "root" where concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) is not null and concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) like \'Firm X%\'',
                    'qualified (derived) properties'),
        unsupported("testOptionalPropertyEmbedded", f"{_SRC}::testOptionalPropertyEmbedded",
                    '???',
                    'no optional-property test in the finder: a missing embedded object is not distinguishable from one with null columns'),
    ],
)


DENORM_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/mapping/embedded/denorm_mapping.md",
    expectations=[
        ported("testDenormMappingOneToManyProjectUsingPaths", f"{_SRC}::testDenormMappingOneToManyProjectUsingPaths",
               'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()])),
        ported("testDenormMappingOneToManyProject", f"{_SRC}::testDenormMappingOneToManyProject",
               'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()])),
        ported("testDenormMappingOneToManyProjectWithFilter", f"{_SRC}::testDenormMappingOneToManyProjectWithFilter",
               'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME = \'Firm A\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()], f.firm().legal_name().eq("Firm A"))),
        ported("testDenormMappingOneToManyProjectWithComplexFilter", f"{_SRC}::testDenormMappingOneToManyProjectWithComplexFilter",
               'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME = \'Firm A\' and "root".PERSON_ADDRESS_NAME is not null and "root".PERSON_ADDRESS_NAME like \'%Palo Alto\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()], f.firm().legal_name().eq("Firm A").and_(f.address().name().ends_with("Palo Alto")))),
        ported("testGroupByEmbeddedProperty", f"{_SRC}::testGroupByEmbeddedProperty",
               'select "root".FIRM_LEGALNAME as "Firm Name", sum(case when ("root".PERSON_ADDRESS_NAME is not null and "root".PERSON_ADDRESS_NAME like \'1%\') then 1.0 else 0.0 end) as "Count" from PERSON_FIRM_DENORM as "root" group by "Firm Name" order by "Firm Name"',
               lambda f: f.find_all(None, None, [f.firm().legal_name(), when(f.address().name().starts_with("1")).then(1.0).otherwise(0.0).sum()]).group_by(f.firm().legal_name()).order_by(f.firm().legal_name().ascending())),
        ported("testExists", f"{_SRC}::testExists",
               'select "root".PERSON_FIRSTNAME as "name", "root".FIRM_ADDRESS_NAME as "address" from PERSON_FIRM_DENORM as "root" where "root".FIRM_ADDRESS_NAME = \'200 west\'',
               lambda f: f.find_all(None, None, [f.first_name(), f.firm().address().name()], f.firm().address().name().eq("200 west"))),
        ported("testIsEmpty", f"{_SRC}::testIsEmpty",
               'select "root".PERSON_FIRSTNAME as "name", "root".FIRM_LEGALNAME as "firm" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME is null',
               lambda f: f.find_all(None, None, [f.first_name(), f.firm().legal_name()], f.firm().legal_name().is_none())),
    ],
)
