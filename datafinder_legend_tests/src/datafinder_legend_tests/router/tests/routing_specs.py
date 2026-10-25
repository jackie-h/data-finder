"""
Ported from legend-engine core_relational ``router/tests/testRouting.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "router/tests/testRouting.pure"


ROUTING_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testRoutingWithSubtypePropagation", f"{_SRC}::testRoutingWithSubtypePropagation",
                    'select concat("persontable_1".FIRSTNAME, \' \', "persontable_1".LASTNAME) as "managerName" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join personTable as "persontable_1" on ("persontable_0".MANAGERID = "persontable_1".ID)',
                    'a mapping that the port does not load'),
    ],
)
