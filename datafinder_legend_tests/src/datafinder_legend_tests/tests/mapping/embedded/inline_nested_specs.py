"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testInlineEmbeddedNested.pure``.

Inline embedded mappings nested in a class mapped to the denormalised table.
Mapping: tests/mapping/embedded/embedded_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testInlineEmbeddedNested.pure"

INLINE_NESTED_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/embedded_mapping.md",
    expectations=[
        unsupported("testProjection", f"{_SRC}::testProjection",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i", "root".ISSUER_ADDRESS_NAME as "a" from PRODUCT_DENORM as "root"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testFilter", f"{_SRC}::testFilter",
                    'select "root".BOND_TYPE as "type", "root".ISSUER_ADDRESS_NAME as "i" from PRODUCT_DENORM as "root" where "root".ISSUER_ADDRESS_NAME = \'addressi1\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testDenormWithComplexFilter", f"{_SRC}::testDenormWithComplexFilter",
                    'select "root".BOND_DETAILS as "name", "root".ISSUER_ADDRESS_NAME as "c1" from PRODUCT_DENORM as "root" where "root".ISSUER_ADDRESS_NAME like \'%i1\' and "root".BOND_DETAILS like \'Bond%\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupBy", f"{_SRC}::testGroupBy",
                    'select "root".HOLDER_ADDRESS_NAME as "c1", sum(case when "root".BOND_TYPE = \'5 years\' then 5.0 else 1.0 end) as "c2" from PRODUCT_DENORM as "root" group by "c1"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupByComplexAgg", f"{_SRC}::testGroupByComplexAgg",
                    'select "root".HOLDER_ADDRESS_NAME as "c1", sum(case when "root".HOLDER_ADDRESS_NAME like \'addressh1%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" group by "c1"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testQualifierProperty", f"{_SRC}::testQualifierProperty",
                    'select "root".BOND_DETAILS as "name", concat(\'D:\', "root".ISSUER_ADDRESS_NAME) as "c2", concat(\'D:\', "root".HOLDER_ADDRESS_NAME) as "c3" from PRODUCT_DENORM as "root" where concat(\'D:\', "root".ISSUER_ADDRESS_NAME) like \'D:addressi1%\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testInlineInEmbedded", f"{_SRC}::testInlineInEmbedded",
                    'select "root".BOND_TYPE as "type", "root".ISSUER_ADDRESS_NAME as "i", "root".HOLDER_ADDRESS_NAME as "h" from PRODUCT_DENORM as "root" where "root".HOLDER_ADDRESS_NAME = \'addressh1\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
    ],
)
