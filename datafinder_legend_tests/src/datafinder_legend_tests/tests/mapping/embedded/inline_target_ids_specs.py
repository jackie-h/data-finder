"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testInlineEmbeddedTargetIds.pure``.

Inline embedded mappings that share the class mapping of their target.
Mapping: tests/mapping/embedded/embedded_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testInlineEmbeddedTargetIds.pure"

INLINE_TARGET_IDS_SPECS = FinderSpec(
    finder_name="ProductFinder",
    mapping_file="tests/mapping/embedded/embedded_mapping.md",
    expectations=[
        unsupported("testProjection", f"{_SRC}::testProjection",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i", "root".HOLDER as "h" from PRODUCT_DENORM as "root"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testSubType", f"{_SRC}::testSubType",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testSubTypeOnPropertyMappedToNonRootInlineSetImpl", f"{_SRC}::testSubTypeOnPropertyMappedToNonRootInlineSetImpl",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testFilter", f"{_SRC}::testFilter",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root" where "root".ISSUER = \'issuer3\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testDenormWithComplexFilter", f"{_SRC}::testDenormWithComplexFilter",
                    'select "root".BOND_DETAILS as "name", "root".ISSUER as "issuer", "root".HOLDER as "holder" from PRODUCT_DENORM as "root" where "root".HOLDER like \'holder3%\' and "root".BOND_DETAILS like \'Super%\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupBy", f"{_SRC}::testGroupBy",
                    'select "root".HOLDER as "holder", sum(case when "root".BOND_TYPE = \'5 years\' then 5.0 else 1.0 end) as "Profit" from PRODUCT_DENORM as "root" group by "holder"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testGroupByComplexAgg", f"{_SRC}::testGroupByComplexAgg",
                    'select "root".HOLDER as "c1", sum(case when "root".HOLDER like \'holder3%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" group by "c1"',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
        unsupported("testQualifierProperty", f"{_SRC}::testQualifierProperty",
                    'select "root".BOND_DETAILS as "name", concat("root".ISSUER, cast("root".ISSUER_TYPE as varchar)) as "c2", concat("root".HOLDER, cast("root".HOLDER_TYPE as varchar)) as "c3" from PRODUCT_DENORM as "root" where concat("root".ISSUER, cast("root".ISSUER_TYPE as varchar)) like \'issuer1%\'',
                    'inline embedded mappings: a class mapped onto the denormalised table with embedded classes inside it'),
    ],
)
