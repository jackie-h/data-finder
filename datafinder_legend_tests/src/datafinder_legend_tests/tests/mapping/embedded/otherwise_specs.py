"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure``.

``Product.bondDetails`` is embedded in the product row for ``description`` and falls back to a join to BOND_DETAIL for
everything else (Legend's ``Otherwise``). Mapping: tests/mapping/embedded/embedded_mapping.md.
"""
from datafinder import constant, when
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, ROWS, EMBEDDED_WITH_JOIN

_SRC = "tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure"


OTHERWISE_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/embedded_mapping.md",
    expectations=[
        ported("otherwiseTestFilter", f"{_SRC}::otherwiseTestFilter",
               'select "root".PRODUCT_NAME as "name", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where "bond_detail_0".TYPE = \'5 years\'',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().type_()], f.bond_details().type_().eq("5 years"))),
        ported("otherwiseTestProjection", f"{_SRC}::otherwiseTestProjection",
               'select "root".PRODUCT_NAME as "name", "root".BOND_DETAILS as "description", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID)',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().description(), f.bond_details().type_()]),
               known_gaps={BACKEND: EMBEDDED_WITH_JOIN, ROWS: EMBEDDED_WITH_JOIN}),
        ported("otherwiseTestDenormWithComplexFilter", f"{_SRC}::otherwiseTestDenormWithComplexFilter",
               'select "root".PRODUCT_NAME as "name", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where "bond_detail_0".TYPE like \'15%\' and "root".BOND_DETAILS like \'Bond%\'',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().type_()], f.bond_details().type_().starts_with("15").and_(f.bond_details().description().starts_with("Bond"))),
               known_gaps={BACKEND: EMBEDDED_WITH_JOIN, ROWS: EMBEDDED_WITH_JOIN}),
        unsupported("otherwiseTestGroupByComplexExpressionEmbeddedAndJoin", f"{_SRC}::otherwiseTestGroupByComplexExpressionEmbeddedAndJoin",
                    'select "root".BOND_DETAILS as "Bond Type", sum(case when "bond_detail_0".TYPE like \'5%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) group by "Bond Type"',
                    'no aggregate of an expression'),
        ported("testProjectionOtherwise", f"{_SRC}::testProjectionOtherwise",
               'select "root".PRODUCT_NAME as "name", "root".BOND_DETAILS as "description" from PRODUCT_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().description()])),
        ported("testProjectionOtherwiseDeepTraversal", f"{_SRC}::testProjectionOtherwiseDeepTraversal",
               'select "party_0".name as "holder" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) left outer join Party as "party_0" on ("bond_detail_0".HOLDERID = "party_0".id)',
               lambda f: f.find_all(None, None, [f.bond_details().holder().name()])),
        unsupported("testProjectionOtherwiseNonPrimitive", f"{_SRC}::testProjectionOtherwiseNonPrimitive",
                    'select "root".PRODUCT_NAME as "name", "party_0".name as "holder", "bondclassificationtable_0".type as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) left outer join Party as "party_0" on ("bond_detail_0".HOLDERID = "party_0".id) left outer join BondClassificationTable as "bondclassificationtable_0" on ("root".PRODUCT_ID = "bondclassificationtable_0".PRODUCT_ID)',
                    'this test uses a second mapping, with a join to a classification table'),
        unsupported("otherwiseTestQualifierProperty", f"{_SRC}::otherwiseTestQualifierProperty",
                    'select "root".PRODUCT_NAME as "name", case when "bond_detail_0".TYPE like \'5%\' then 5 else 0 end as "duration" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where case when "bond_detail_0".TYPE like \'5%\' then 5 else 0 end = \'5\'',
                    'no qualified (derived) properties'),
        unsupported("otherwiseTestQualifierPropertyExpressionWithPropertyInJoinOnly", f"{_SRC}::otherwiseTestQualifierPropertyExpressionWithPropertyInJoinOnly",
                    'select "root".PRODUCT_NAME as "name", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where "bond_detail_0".TYPE like \'5%\'',
                    'no qualified (derived) properties'),
        unsupported("otherwiseTestQualifierPropertyExpressionWithEmbeddedProperty", f"{_SRC}::otherwiseTestQualifierPropertyExpressionWithEmbeddedProperty",
                    'select "root".PRODUCT_NAME as "name", concat(\'la\', "root".BOND_DETAILS) as "description" from PRODUCT_DENORM as "root" where concat(\'\', "root".BOND_DETAILS) <> \'2\' or concat(\'\', "root".BOND_DETAILS) is null',
                    'no qualified (derived) properties'),
        unsupported("otherwiseTestQualifierPropertyExpressionWithEmbeddedPropertyandJoinProperty", f"{_SRC}::otherwiseTestQualifierPropertyExpressionWithEmbeddedPropertyandJoinProperty",
                    'select concat("bond_detail_0".TYPE, \' \', "root".BOND_DETAILS) as "fullName" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID)',
                    'no qualified (derived) properties'),
        unsupported("otherwiseTestComplexExpression", f"{_SRC}::otherwiseTestComplexExpression",
                    'select "root".PRODUCT_NAME as "name", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where case when "bond_detail_0".TYPE like \'5%\' then true else false end',
                    'no conditional as a filter'),
        unsupported("otherwiseTestComplexExpressionWithEnumMapping", f"{_SRC}::otherwiseTestComplexExpressionWithEnumMapping",
                    'select "root".BOND_STATUS as "status", "bond_detail_0".TYPE as "type" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where "root".BOND_STATUS in (\'CLOSED\', \'Closed\')',
                    'no enumeration mapping'),
        unsupported("otherwiseTestEmbeddedToEmbedded", f"{_SRC}::otherwiseTestEmbeddedToEmbedded",
                    'select \'test\' as "name" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID)',
                    'an embedded property mapped to a constant'),
        unsupported("otherwiseTestFilterExistsOnEmbeddedProperty", f"{_SRC}::otherwiseTestFilterExistsOnEmbeddedProperty",
                    'select "root".PRODUCT_NAME as "name" from PRODUCT_DENORM as "root" where "root".BOND_DETAILS like \'Bond%\'',
                    'no exists over an embedded property'),
        unsupported("otherwiseTestProjectExistsOnEmbeddedProperty", f"{_SRC}::otherwiseTestProjectExistsOnEmbeddedProperty",
                    'select "root".BOND_DETAILS like \'Bond%\' as "descriptionExists" from PRODUCT_DENORM as "root"',
                    'no exists over an embedded property'),
        unsupported("otherwiseTestFilterExistsOnOtherwiseProperty", f"{_SRC}::otherwiseTestFilterExistsOnOtherwiseProperty",
                    'select "root".PRODUCT_NAME as "name" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) where "bond_detail_0".TYPE = \'15 years\'',
                    'no exists over an embedded property'),
        unsupported("otherwiseTestProjectExistsOnOtherwiseProperty", f"{_SRC}::otherwiseTestProjectExistsOnOtherwiseProperty",
                    'select "bond_detail_0".TYPE = \'15 years\' as "typeExists" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID)',
                    'no exists over an embedded property'),
        ported("otherwiseTestGroupBy", f"{_SRC}::otherwiseTestGroupBy",
               'select "bond_detail_0".TYPE as "Bond Type", sum(case when "root".MARKET_NAME = \'LSE\' then 5.0 else 1.0 end) as "Profit" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) group by "Bond Type"',
               lambda f: f.find_all(None, None, [f.bond_details().type_(), when(f.market().eq("LSE")).then(5.0).otherwise(1.0).sum()]).group_by(f.bond_details().type_())),
        ported("otherwiseTestGroupByComplexAgg", f"{_SRC}::otherwiseTestGroupByComplexAgg",
               'select "bond_detail_0".TYPE as "Bond Type", sum(case when "bond_detail_0".TYPE like \'5%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" left outer join BOND_DETAIL as "bond_detail_0" on ("root".PRODUCT_ID = "bond_detail_0".BOND_ID) group by "Bond Type"',
               lambda f: f.find_all(None, None, [f.bond_details().type_(), when(f.bond_details().type_().starts_with("5")).then(5.0).otherwise(1.0).sum()]).group_by(f.bond_details().type_())),
    ],
)
