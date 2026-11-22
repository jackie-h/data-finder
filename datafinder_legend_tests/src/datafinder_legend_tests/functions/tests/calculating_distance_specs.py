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
                    'select "root".name as "name", cast(6371.0 as float) * cast(2.0 as float) * atan2(sqrt(power(sin((1.0 * (((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) - ((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180))) / 2), 2) + cos((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180) * cos((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) * power(sin((1.0 * (((1.0 * cast(-0.127706 as float) * cast(3.141592653589793 as float)) / 180) - ((1.0 * "root".longitude * cast(3.141592653589793 as float)) / 180))) / 2), 2)), sqrt(1 - (power(sin((1.0 * (((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) - ((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180))) / 2), 2) + cos((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180) * cos((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) * power(sin((1.0 * (((1.0 * cast(-0.127706 as float) * cast(3.141592653589793 as float)) / 180) - ((1.0 * "root".longitude * cast(3.141592653589793 as float)) / 180))) / 2), 2)))) as "distance" from coordinatesTable as "root"',
                    'distance calculation functions'),
        unsupported("testDistanceSphericalLawOfCosinesInProject", f"{_SRC}::testDistanceSphericalLawOfCosinesInProject",
                    'select "root".name as "name", cast(6371.0 as float) * acos(sin((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180) * sin((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) + cos((1.0 * "root".latitude * cast(3.141592653589793 as float)) / 180) * cos((1.0 * cast(51.507356 as float) * cast(3.141592653589793 as float)) / 180) * cos(((1.0 * cast(-0.127706 as float) * cast(3.141592653589793 as float)) / 180) - ((1.0 * "root".longitude * cast(3.141592653589793 as float)) / 180))) as "distance" from coordinatesTable as "root"',
                    'distance calculation functions'),
    ],
)
