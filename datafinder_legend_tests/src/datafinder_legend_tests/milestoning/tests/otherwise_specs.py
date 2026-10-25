"""
Ported from legend-engine core_relational ``milestoning/tests/testOtherwise.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testOtherwise.pure"


OTHERWISE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testAssoWithOtherwise", f"{_SRC}::testAssoWithOtherwise",
                    'select "productclassificationtable_0".type_description as "ok" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".type = \'STOCK\' and "productclassificationtable_0".thru_z = \'9999-12-31 00:00:00.0000\') where "root".type = \'STOCK\' and "root".thru_z = \'9999-12-31 00:00:00.0000\'',
                    'milestoning with an Otherwise embedded mapping'),
        unsupported("testAssoWithOtherwiseDeep", f"{_SRC}::testAssoWithOtherwiseDeep",
                    'select "productexchangetable_0".name as "ok" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".type = \'STOCK\' and "productclassificationtable_0".from_z <= \'2008-10-10\' and "productclassificationtable_0".thru_z > \'2008-10-10\') left outer join (select "productexchangetable_1".name as name from ProductExchangeTable as "productexchangetable_1" where "productexchangetable_1".from_z <= \'2009-12-10\' and "productexchangetable_1".thru_z > \'2009-12-10\') as "productexchangetable_0" on ("productclassificationtable_0".exchange = "productexchangetable_0".name) where "root".type = \'STOCK\' and "root".thru_z = \'9999-12-31 00:00:00.0000\'',
                    'milestoning with an Otherwise embedded mapping'),
    ],
)
