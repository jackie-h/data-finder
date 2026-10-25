"""
Ported from legend-engine core_relational ``tests/mapping/inheritance/testSubtypeMapping.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/inheritance/testSubtypeMapping.pure"


SUBTYPE_MAPPING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testSubTypeMappingValidWhenMappedExplicitly", f"{_SRC}::testSubTypeMappingValidWhenMappedExplicitly",
                    'select "product_rating_1".RATING as "description" from PRODUCT_RATING as "root" left outer join productSchema.productTable as "producttable_0" on ("producttable_0".ID = "root".PRODUCT_ID) left outer join PRODUCT_RATING as "product_rating_1" on ("producttable_0".ID = "product_rating_1".PRODUCT_ID)',
                    'a mapping that the port does not load'),
    ],
)
