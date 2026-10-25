"""
Ported from legend-engine core_relational ``tests/mapping/association/testAssociationMappingInheritance.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/association/testAssociationMappingInheritance.pure"


ASSOCIATION_MAPPING_INHERITANCE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testBuilderRoutingOfAggFunctionParameters", f"{_SRC}::testBuilderRoutingOfAggFunctionParameters",
                    'select "root".name as "name", sum("car_0".db_car_wheelCount) as "wheelCountSum" from Person as "root" left outer join Car as "car_0" on ("root".ID = "car_0".c_PersonID and "car_0".c_Description = \'\') group by "name"',
                    'an association mapping in an inheritance mapping'),
    ],
)
