"""
Ported from legend-engine core_relational ``functions/tests/testCalculatingDistance.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testCalculatingDistance.pure"


CALCULATING_DISTANCE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testDistanceHaversineInProject", f"{_SRC}::testDistanceHaversineInProject",
                    'select "root".name as "name", (6371.0 * (2.0 * atan2(sqrt((power(sin(((1.0 * (((1.0 * (51.507356 * 3.141592653589793)) / 180) - ((1.0 * ("root".latitude * 3.141592653589793)) / 180))) / 2)), 2) + (cos(((1.0 * ("root".latitude * 3.141592653589793)) / 180)) * cos(((1.0 * (51.507356 * 3.141592653589793)) / 180)) * power(sin(((1.0 * (((1.0 * (-0.127706 * 3.141592653589793)) / 180) - ((1.0 * ("root".longitude * 3.141592653589793)) / 180))) / 2)), 2)))),sqrt((1 - (power(sin(((1.0 * (((1.0 * (51.507356 * 3.141592653589793)) / 180) - ((1.0 * ("root".latitude * 3.141592653589793)) / 180))) / 2)), 2) + (cos(((1.0 * ("root".latitude * 3.141592653589793)) / 180)) * cos(((1.0 * (51.507356 * 3.141592653589793)) / 180)) * power(sin(((1.0 * (((1.0 * (-0.127706 * 3.141592653589793)) / 180) - ((1.0 * ("root".longitude * 3.141592653589793)) / 180))) / 2)), 2)))))))) as "distance" from coordinatesTable as "root"',
                    'distance calculation functions'),
        unsupported("testDistanceSphericalLawOfCosinesInProject", f"{_SRC}::testDistanceSphericalLawOfCosinesInProject",
                    'select "root".name as "name", (6371.0 * acos(((sin(((1.0 * ("root".latitude * 3.141592653589793)) / 180)) * sin(((1.0 * (51.507356 * 3.141592653589793)) / 180))) + (cos(((1.0 * ("root".latitude * 3.141592653589793)) / 180)) * cos(((1.0 * (51.507356 * 3.141592653589793)) / 180)) * cos((((1.0 * (-0.127706 * 3.141592653589793)) / 180) - ((1.0 * ("root".longitude * 3.141592653589793)) / 180))))))) as "distance" from coordinatesTable as "root"',
                    'distance calculation functions'),
    ],
)
