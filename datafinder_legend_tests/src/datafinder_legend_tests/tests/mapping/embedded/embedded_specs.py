"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testEmbeddedMapping.pure``.

Properties embedded in a denormalised table with nothing to fall back to.
Mapping: tests/mapping/embedded/embedded_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testEmbeddedMapping.pure"

EMBEDDED_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/embedded_mapping.md",
    expectations=[
        unsupported("testDenormMappingOneToManyProjectUsingPaths", f"{_SRC}::testDenormMappingOneToManyProjectUsingPaths",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProject", f"{_SRC}::testDenormMappingOneToManyProject",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectLambdaSyntaxWithMap", f"{_SRC}::testDenormMappingOneToManyProjectLambdaSyntaxWithMap",
                    'select "person_firm_denorm_2".PERSON_ADDRESS_NAME as "address" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1") as "root" left outer join PERSON_FIRM_DENORM as "person_firm_denorm_2" on ("root".FIRM_ID = "person_firm_denorm_2".FIRM_ID) order by "address" asc',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectWithFilter", f"{_SRC}::testDenormMappingOneToManyProjectWithFilter",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME = \'Firm A\'',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectWithComplexFilter", f"{_SRC}::testDenormMappingOneToManyProjectWithComplexFilter",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_NAME as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_NAME as "firmAddress" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME = \'Firm A\' and "root".PERSON_ADDRESS_NAME is not null and "root".PERSON_ADDRESS_NAME like \'%Palo Alto\'',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectWithEnum", f"{_SRC}::testDenormMappingOneToManyProjectWithEnum",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectWithFilterOnEnumLeft", f"{_SRC}::testDenormMappingOneToManyProjectWithFilterOnEnumLeft",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root" where "root".PERSON_ADDRESS_TYPE = 1',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectWithFilterOnEnumRight", f"{_SRC}::testDenormMappingOneToManyProjectWithFilterOnEnumRight",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_TYPE as "address" from PERSON_FIRM_DENORM as "root" where "root".PERSON_ADDRESS_TYPE = 1',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testGroupByEmbeddedProperty", f"{_SRC}::testGroupByEmbeddedProperty",
                    'select "root".FIRM_LEGALNAME as "Firm Name", sum(case when ("root".PERSON_ADDRESS_NAME is not null and "root".PERSON_ADDRESS_NAME like \'1%\') then 1.0 else 0.0 end) as "Count" from PERSON_FIRM_DENORM as "root" group by "Firm Name" order by "Firm Name"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testProjectToEmbedded", f"{_SRC}::testProjectToEmbedded",
                    'select "root".FIRM_LEGALNAME as "legalName", "person_firm_denorm_2".PERSON_ADDRESS_NAME as "address" from (select distinct "person_firm_denorm_1".FIRM_LEGALNAME as FIRM_LEGALNAME, "person_firm_denorm_1".FIRM_ID as FIRM_ID from PERSON_FIRM_DENORM as "person_firm_denorm_1") as "root" left outer join PERSON_FIRM_DENORM as "person_firm_denorm_2" on ("root".FIRM_ID = "person_firm_denorm_2".FIRM_ID) order by "legalName" asc, "address" asc',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingOneToManyProjectEmbeddedQualifier", f"{_SRC}::testDenormMappingOneToManyProjectEmbeddedQualifier",
                    'select "root".PERSON_FIRSTNAME as "firstName", concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) as "nameAndAddress" from PERSON_FIRM_DENORM as "root"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testDenormMappingWithQualifierWithIfAndEquals", f"{_SRC}::testDenormMappingWithQualifierWithIfAndEquals",
                    'select "root".PERSON_FIRSTNAME as "name", case when "root".FIRM_LEGALNAME = \'Firm X\' then \'Yes\' else \'No\' end as "FirmX" from PERSON_FIRM_DENORM as "root"',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testFilterWithEmbeddedQualifier", f"{_SRC}::testFilterWithEmbeddedQualifier",
                    'select "root".PERSON_FIRSTNAME as "firstName", concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) as "nameAndAddress" from PERSON_FIRM_DENORM as "root" where concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) is not null and concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) like \'Firm X%\'',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testExists", f"{_SRC}::testExists",
                    'select "root".PERSON_FIRSTNAME as "name", "root".FIRM_ADDRESS_NAME as "address" from PERSON_FIRM_DENORM as "root" where "root".FIRM_ADDRESS_NAME = \'200 west\'',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testIsEmpty", f"{_SRC}::testIsEmpty",
                    'select "root".PERSON_FIRSTNAME as "name", "root".FIRM_LEGALNAME as "firm" from PERSON_FIRM_DENORM as "root" where "root".FIRM_LEGALNAME is null',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testMapEmbeddedQualifierWithIfTwoEmbeddedProperties", f"{_SRC}::testMapEmbeddedQualifierWithIfTwoEmbeddedProperties",
                    'select case when "root".FIRM_LEGALNAME = \'Firm X\' then concat("root".FIRM_LEGALNAME, \' , Top Secret\') else concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) end as "nameAndMaskedAddress" from PERSON_FIRM_DENORM as "root" where concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) is not null and concat("root".FIRM_LEGALNAME, \',\', "root".FIRM_ADDRESS_NAME) like \'Firm X%\'',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
        unsupported("testOptionalPropertyEmbedded", f"{_SRC}::testOptionalPropertyEmbedded",
                    '???',
                    'an embedded mapping needs a real join to fall back to; this one has none'),
    ],
)
