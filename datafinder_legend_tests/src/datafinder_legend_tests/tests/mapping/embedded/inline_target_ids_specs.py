"""
Ported from legend-engine core_relational ``tests/mapping/embedded/testInlineEmbeddedTargetIds.pure``.

Inline embedded mappings that share the class mapping of their target.
Mapping: tests/mapping/embedded/inline_bond_detail_mapping.md.
"""
from datafinder import when
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/embedded/testInlineEmbeddedTargetIds.pure"

INLINE_TARGET_IDS_SPECS = FinderSpec(
    finder_name="BondDetailFinder",
    mapping_file="tests/mapping/embedded/inline_bond_detail_mapping.md",
    expectations=[
        ported("testProjection", f"{_SRC}::testProjection",
               'select "root".BOND_TYPE as "type", "root".ISSUER as "i", "root".HOLDER as "h" from PRODUCT_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.type_(), f.issuer().name(), f.holder().name()])),
        ported("testSubType", f"{_SRC}::testSubType",
               'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root"',
               lambda f: f.find_all(None, None, [f.type_(), f.issuer().name()])),
        unsupported("testSubTypeOnPropertyMappedToNonRootInlineSetImpl", f"{_SRC}::testSubTypeOnPropertyMappedToNonRootInlineSetImpl",
                    'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root"',
                    'Legend marks this test ToFix: it queries the holder but asserts SQL that reads the issuer, so there is no SQL to match'),
        ported("testFilter", f"{_SRC}::testFilter",
               'select "root".BOND_TYPE as "type", "root".ISSUER as "i" from PRODUCT_DENORM as "root" where "root".ISSUER = \'issuer3\'',
               lambda f: f.find_all(None, None, [f.type_(), f.issuer().name()], f.issuer().name().eq("issuer3"))),
        ported("testDenormWithComplexFilter", f"{_SRC}::testDenormWithComplexFilter",
               'select "root".BOND_DETAILS as "name", "root".ISSUER as "issuer", "root".HOLDER as "holder" from PRODUCT_DENORM as "root" where "root".HOLDER like \'holder3%\' and "root".BOND_DETAILS like \'Super%\'',
               lambda f: f.find_all(None, None, [f.description(), f.issuer().name(), f.holder().name()], f.holder().name().starts_with("holder3").and_(f.description().starts_with("Super")))),
        ported("testGroupBy", f"{_SRC}::testGroupBy",
               'select "root".HOLDER as "holder", sum(case when "root".BOND_TYPE = \'5 years\' then 5.0 else 1.0 end) as "Profit" from PRODUCT_DENORM as "root" group by "holder"',
               lambda f: f.find_all(None, None, [f.holder().name(), when(f.type_().eq("5 years")).then(5.0).otherwise(1.0).sum()]).group_by(f.holder().name())),
        ported("testGroupByComplexAgg", f"{_SRC}::testGroupByComplexAgg",
               'select "root".HOLDER as "c1", sum(case when "root".HOLDER like \'holder3%\' then 5.0 else 1.0 end) as "sum" from PRODUCT_DENORM as "root" group by "c1"',
               lambda f: f.find_all(None, None, [f.holder().name(), when(f.holder().name().starts_with("holder3")).then(5.0).otherwise(1.0).sum()]).group_by(f.holder().name())),
        unsupported("testQualifierProperty", f"{_SRC}::testQualifierProperty",
                    'select "root".BOND_DETAILS as "name", concat("root".ISSUER, cast("root".ISSUER_TYPE as varchar)) as "c2", concat("root".HOLDER, cast("root".HOLDER_TYPE as varchar)) as "c3" from PRODUCT_DENORM as "root" where concat("root".ISSUER, cast("root".ISSUER_TYPE as varchar)) like \'issuer1%\'',
                    'no qualified (derived) properties, and the party type is an enumeration, which the model does not have'),
    ],
)
