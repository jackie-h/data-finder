"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testInlineEmbeddedMapping.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testInlineEmbeddedMapping.pure"


INLINE_EMBEDDED_MAPPING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjection", f"{_SRC}::testProjection",
                    'select "root".PRODUCT_NAME as "name", "root".BOND_DETAILS as "description" from PRODUCT_DENORM as "root"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testFilter", f"{_SRC}::testFilter",
                    'select "root".PRODUCT_NAME as "name", "root".BOND_TYPE as "type" from PRODUCT_DENORM as "root" where "root".BOND_TYPE = \'7 weeks\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testDenormWithComplexFilter", f"{_SRC}::testDenormWithComplexFilter",
                    'select "root".PRODUCT_NAME as "name", "root".BOND_TYPE as "type" from PRODUCT_DENORM as "root" where "root".BOND_TYPE like \'5%\' and "root".BOND_DETAILS like \'Bond%\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupBy", f"{_SRC}::testGroupBy",
                    'select "root".BOND_TYPE as "Bond Type", sum(case when "root".MARKET_NAME = \'LSE\' then 5.0 else 1.0 end) as "Profit" from PRODUCT_DENORM as "root" group by "Bond Type"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupByComplexAgg", f"{_SRC}::testGroupByComplexAgg",
                    'select "root".BOND_TYPE as "Bond Type", sum(case when "root".BOND_TYPE like \'5%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" group by "Bond Type"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testQualifierProperty", f"{_SRC}::testQualifierProperty",
                    'select "root".PRODUCT_NAME as "name", case when "root".BOND_TYPE like \'5%\' then 5 else 0 end as "duration" from PRODUCT_DENORM as "root" where case when "root".BOND_TYPE like \'5%\' then 5 else 0 end = \'5\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testInlineEmbeddedMappingWithAssociationFromRootMapping", f"{_SRC}::testInlineEmbeddedMappingWithAssociationFromRootMapping",
                    'select "root".PRODUCT_NAME as "name", "bondclassificationtable_0".type as "bondClassification" from PRODUCT_DENORM as "root" left outer join BondClassificationTable as "bondclassificationtable_0" on ("root".PRODUCT_ID = "bondclassificationtable_0".PRODUCT_ID) where "root".PRODUCT_NAME = \'Product 1\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
    ],
)
