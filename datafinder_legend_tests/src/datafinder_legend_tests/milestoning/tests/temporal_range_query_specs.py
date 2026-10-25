"""
Ported from legend-engine core_relational ``milestoning/tests/testTemporalRangeQuery.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "milestoning/tests/testTemporalRangeQuery.pure"


TEMPORAL_RANGE_QUERY_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testBusinessTemporalRangeQueryOnRoot", f"{_SRC}::testBusinessTemporalRangeQueryOnRoot",
                    'select distinct "root".type as "type", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root" where "root".type = \'STOCK\' and "root".from_z <= \'2015-09-01\' and "root".thru_z > \'2015-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnRootAndProperty", f"{_SRC}::testBusinessTemporalRangeQueryOnRootAndProperty",
                    'select distinct "productclassificationtable_0".type as "type", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2015-09-01\' and "productclassificationtable_0".thru_z > \'2015-01-01\') where "productclassificationtable_0".type = \'STOCK\' and "root".from_z <= \'2015-09-01\' and "root".thru_z > \'2015-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnRootAndPropertyDeep", f"{_SRC}::testBusinessTemporalRangeQueryOnRootAndPropertyDeep",
                    'select distinct "productexchangetable_1".name as "name", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2015-09-01\' and "productclassificationtable_0".thru_z > \'2015-01-01\') left outer join ProductExchangeTable as "productexchangetable_0" on ("productclassificationtable_0".exchange = "productexchangetable_0".name and "productexchangetable_0".from_z <= \'2015-09-01\' and "productexchangetable_0".thru_z > \'2015-01-01\') left outer join (select "productexchangetable_2".name as name from ProductExchangeTable as "productexchangetable_2" where "productexchangetable_2".from_z <= \'2015-09-01\' and "productexchangetable_2".thru_z > \'2015-01-01\') as "productexchangetable_1" on ("productclassificationtable_0".exchange = "productexchangetable_1".name) where "productexchangetable_0".name = \'LNSE\' and "root".from_z <= \'2015-09-01\' and "root".thru_z > \'2015-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnProperty", f"{_SRC}::testBusinessTemporalRangeQueryOnProperty",
                    'select distinct "productclassificationtable_0".type as "type", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2015-09-01\' and "productclassificationtable_0".thru_z > \'2015-01-01\') where "productclassificationtable_0".type = \'STOCK\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnPropertyDeep", f"{_SRC}::testBusinessTemporalRangeQueryOnPropertyDeep",
                    'select distinct "productexchangetable_1".name as "name", "root".from_z as "from", "root".thru_z as "thru" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= \'2015-09-01\' and "productclassificationtable_0".thru_z > \'2015-01-01\') left outer join ProductExchangeTable as "productexchangetable_0" on ("productclassificationtable_0".exchange = "productexchangetable_0".name and "productexchangetable_0".from_z <= \'2015-09-01\' and "productexchangetable_0".thru_z > \'2015-01-01\') left outer join (select "productexchangetable_2".name as name from ProductExchangeTable as "productexchangetable_2" where "productexchangetable_2".from_z <= \'2015-09-01\' and "productexchangetable_2".thru_z > \'2015-01-01\') as "productexchangetable_1" on ("productclassificationtable_0".exchange = "productexchangetable_1".name) where "productexchangetable_0".name = \'LNSE\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testProcessingTemporalRangeQueryOnRoot", f"{_SRC}::testProcessingTemporalRangeQueryOnRoot",
                    'select distinct "root".name as "name", "root".in_z as "in", "root".out_z as "out" from CertificationTable as "root" where ("root".name = \'APCE\' or "root".name = \'SRCE\') and "root".in_z <= \'1982-01-01\' and "root".out_z > \'1979-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testProcessingTemporalRangeQueryOnProperty", f"{_SRC}::testProcessingTemporalRangeQueryOnProperty",
                    'select distinct "tradertable_0".kerberos as "kerberos", "root".in_z as "in", "root".out_z as "out" from CertificationTable as "root" left outer join TraderTable as "tradertable_0" on ("tradertable_0".kerberos = "root".kerberos and "tradertable_0".in_z <= \'2014-01-01\' and "tradertable_0".out_z > \'2012-01-01\') where "tradertable_0".kerberos = \'ggekko\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testProcessingTemporalRangeQueryOnRootAndProperty", f"{_SRC}::testProcessingTemporalRangeQueryOnRootAndProperty",
                    'select distinct "tradertable_0".kerberos as "kerberos", "root".in_z as "in", "root".out_z as "out" from CertificationTable as "root" left outer join TraderTable as "tradertable_0" on ("tradertable_0".kerberos = "root".kerberos and "tradertable_0".in_z <= \'2014-01-01\' and "tradertable_0".out_z > \'2012-01-01\') where "tradertable_0".kerberos = \'ggekko\' and "root".in_z <= \'1982-01-01\' and "root".out_z > \'1979-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithFunction", f"{_SRC}::testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithFunction",
                    'select distinct "tradertable_0".kerberos as "kerberos" from CertificationTable as "root" left outer join TraderTable as "tradertable_0" on ("tradertable_0".kerberos = "root".kerberos and "tradertable_0".in_z <= \'2015-01-01\' and "tradertable_0".out_z > \'2015-01-01\') where "root".in_z <= \'1982-01-01\' and "root".out_z > \'1979-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithThisBusinessDate", f"{_SRC}::testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithThisBusinessDate",
                    'select distinct "productclassificationtable_0".type as "classificationType" from ProductTable as "root" left outer join ProductClassificationTable as "productclassificationtable_0" on ("root".type = "productclassificationtable_0".type and "productclassificationtable_0".from_z <= "root".from_z and "productclassificationtable_0".thru_z > "root".from_z) where "root".from_z <= \'1982-01-01\' and "root".thru_z > \'1979-01-01\'',
                    'range queries over a milestoned property (all versions between two dates)'),
        unsupported("testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithFunctionCallWithThisBusinessDateParameter", f"{_SRC}::testBusinessTemporalRangeQueryOnRootWithSubsequentCallToMilestonedQualifiedPropertyWithFunctionCallWithThisBusinessDateParameter",
                    '',
                    'range queries over a milestoned property (all versions between two dates)'),
    ],
)
