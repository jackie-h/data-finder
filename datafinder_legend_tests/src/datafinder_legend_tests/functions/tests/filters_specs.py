"""
Ported from legend-engine core_relational ``functions/tests/testFilters.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testFilters.pure"


FILTERS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testFilterBeforeAndAfterGroupBy", f"{_SRC}::testFilterBeforeAndAfterGroupBy",
                    'select "root".FIRSTNAME as "name", sum("root".AGE) as "age" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) where "firmtable_0".LEGALNAME = \'Firm X\' and "addresstable_0".NAME = \'New York\' group by "name" having "root".FIRSTNAME <> \'Hello\' or "root".FIRSTNAME is null',
                    'filters in the class mapping'),
        unsupported("testFilterBeforeAndAfterProject", f"{_SRC}::testFilterBeforeAndAfterProject",
                    'select "root".FIRSTNAME as "name" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_0".ADDRESSID) where "firmtable_0".LEGALNAME = \'Firm X\' and "addresstable_0".NAME = \'New York\' and ("root".FIRSTNAME <> \'Hello\' or "root".FIRSTNAME is null)',
                    'filters in the class mapping'),
    ],
)
