"""
Ported from legend-engine core_relational ``tests/advanced/testFailures.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/advanced/testFailures.pure"


FAILURES_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("failMoveFilterInOnClauseJoin", f"{_SRC}::failMoveFilterInOnClauseJoin",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "cusip" from productTable as "root" left outer join (select "synonymtable_1".PRODID as PRODID, "synonymtable_1".NAME as NAME from synonymTable as "synonymtable_1" left outer join synonymType as "synonymtype_0" on ("synonymtable_1".ID = "synonymtype_0".ID) where "synonymtype_0".TYPE = \'CUSIP\') as "synonymtable_0" on ("root".ID = "synonymtable_0".PRODID)',
                    'queries that fail in Legend'),
        unsupported("failMoveFilterInOnClauseModel", f"{_SRC}::failMoveFilterInOnClauseModel",
                    'select "root".NAME as "name", "synonymtable_0".NAME as "cusip" from productTable as "root" left outer join (select "synonymtable_1".PRODID as PRODID, "synonymtable_1".NAME as NAME from synonymTable as "synonymtable_1" left outer join synonymType as "synonymtype_0" on ("synonymtable_1".ID = "synonymtype_0".ID) where "synonymtype_0".TYPE = \'CUSIP\') as "synonymtable_0" on ("root".ID = "synonymtable_0".PRODID)',
                    'queries that fail in Legend'),
    ],
)
