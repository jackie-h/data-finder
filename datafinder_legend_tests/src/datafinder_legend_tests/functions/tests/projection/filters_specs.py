"""
Ported from legend-engine core_relational ``functions/tests/projection/testFilters.pure`` and ``testFilter.pure``.

Only the Person filters are ported; the Trade, Interaction and mapping-specific tests need model and mapping
features the simple subset does not have. Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported

_FILTERS = "functions/tests/projection/testFilters.pure"
_FILTER = "functions/tests/projection/testFilter.pure"

_OPTIONAL_PROPERTY_GAP = (
    "Legend adds `AGE is not null` before a comparison on an optional property; the model has no multiplicity, "
    "and the extra predicate does not change the result"
)

FILTERS_PERSON_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testGreaterThanWithOptionalProperty", f"{_FILTERS}::testGreaterThanWithOptionalProperty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is not null and "root".AGE > 30',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()], f.age() > 30),
               known_gaps={"duckdb": _OPTIONAL_PROPERTY_GAP}),
        ported("testGreaterThanWithOptionalPropertyIsEmpty", f"{_FILTERS}::testGreaterThanWithOptionalPropertyIsEmpty",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE is null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()], f.age().is_none())),
        ported("testIsNullColumn", f"{_FILTER}::testIsNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()],
                                    (f.age() > 30).and_op(f.age().is_none()))),
        ported("testIsNotNullColumn", f"{_FILTER}::testIsNotNullColumn",
               'select "root".FIRSTNAME as "firstName", "root".AGE as "age" from personTable as "root" '
               'where "root".AGE > 30 and "root".AGE is not null',
               lambda f: f.find_all(None, None, [f.first_name(), f.age()],
                                    (f.age() > 30).and_op(f.age().is_not_none()))),
    ],
)
