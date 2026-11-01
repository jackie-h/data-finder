"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testInlineEmbeddedMapping.pure``.

Mapping: tests/mapping/embedded/inline_product_mapping.md.
"""
from datafinder import when
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testInlineEmbeddedMapping.pure"


INLINE_EMBEDDED_MAPPING_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/inline_product_mapping.md",
    expectations=[
        ported("testProjection", f"{_SRC}::testProjection",
               'select "root".PRODUCT_NAME as "name", "root".BOND_DETAILS as "description" from PRODUCT_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().description()])),
        ported("testFilter", f"{_SRC}::testFilter",
               'select "root".PRODUCT_NAME as "name", "root".BOND_TYPE as "type" from PRODUCT_DENORM as "root" where "root".BOND_TYPE = \'7 weeks\'',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().type_()], f.bond_details().type_().eq("7 weeks"))),
        ported("testDenormWithComplexFilter", f"{_SRC}::testDenormWithComplexFilter",
               'select "root".PRODUCT_NAME as "name", "root".BOND_TYPE as "type" from PRODUCT_DENORM as "root" where "root".BOND_TYPE like \'5%\' and "root".BOND_DETAILS like \'Bond%\'',
               lambda f: f.find_all(None, None, [f.name(), f.bond_details().type_()], f.bond_details().type_().starts_with("5").and_(f.bond_details().description().starts_with("Bond")))),
        ported("testGroupBy", f"{_SRC}::testGroupBy",
               'select "root".BOND_TYPE as "Bond Type", sum(case when "root".MARKET_NAME = \'LSE\' then 5.0 else 1.0 end) as "Profit" from PRODUCT_DENORM as "root" group by "Bond Type"',
               lambda f: f.find_all(None, None, [f.bond_details().type_(), when(f.market().eq("LSE")).then(5.0).otherwise(1.0).sum()]).group_by(f.bond_details().type_())),
        ported("testGroupByComplexAgg", f"{_SRC}::testGroupByComplexAgg",
               'select "root".BOND_TYPE as "Bond Type", sum(case when "root".BOND_TYPE like \'5%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" group by "Bond Type"',
               lambda f: f.find_all(None, None, [f.bond_details().type_(), when(f.bond_details().type_().starts_with("5")).then(5.0).otherwise(1.0).sum()]).group_by(f.bond_details().type_())),
        unsupported("testQualifierProperty", f"{_SRC}::testQualifierProperty",
                    'select "root".PRODUCT_NAME as "name", case when "root".BOND_TYPE like \'5%\' then 5 else 0 end as "duration" from PRODUCT_DENORM as "root" where case when "root".BOND_TYPE like \'5%\' then 5 else 0 end = \'5\'',
                    'no qualified (derived) properties'),
        unsupported("testInlineEmbeddedMappingWithAssociationFromRootMapping", f"{_SRC}::testInlineEmbeddedMappingWithAssociationFromRootMapping",
                    'select "root".PRODUCT_NAME as "name", "bondclassificationtable_0".type as "bondClassification" from PRODUCT_DENORM as "root" left outer join BondClassificationTable as "bondclassificationtable_0" on ("root".PRODUCT_ID = "bondclassificationtable_0".PRODUCT_ID) where "root".PRODUCT_NAME = \'Product 1\'',
                    'a join from an embedded object (bondDetails.bondClassification) to a table'),
    ],
)
