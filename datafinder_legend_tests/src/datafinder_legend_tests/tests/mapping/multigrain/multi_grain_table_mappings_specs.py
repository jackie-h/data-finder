"""
Ported from legend-engine core_relational ``tests/mapping/multigrain/testMultiGrainTableMappings.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/mapping/multigrain/testMultiGrainTableMappings.pure"


MULTI_GRAIN_TABLE_MAPPINGS_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testProjectPerson", f"{_SRC}::testProjectPerson",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_LINE_1 as "address", "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_LINE_1 as "firmAddress" from PERSON_FIRM_ADDRESS_MULTIGRAIN as "root" where "root".DLEVEL = \'P\'',
                    'a class mapping filter (~filter) over a multigrain table'),
        unsupported("testProjectFirm", f"{_SRC}::testProjectFirm",
                    'select "root".FIRM_LEGALNAME as "legalName", "root".FIRM_ADDRESS_LINE_1 as "address" from PERSON_FIRM_ADDRESS_MULTIGRAIN as "root" where "root".DLEVEL = \'F\'',
                    'a class mapping filter (~filter) over a multigrain table'),
        unsupported("testProjectPersonWithJoinToAddress", f"{_SRC}::testProjectPersonWithJoinToAddress",
                    'select "root".PERSON_FIRSTNAME as "firstName", "root".PERSON_ADDRESS_LINE_1 as "address", "person_firm_address_multigrain_1".ADDRESS_TYPE as "addressType" from PERSON_FIRM_ADDRESS_MULTIGRAIN as "root" left outer join PERSON_FIRM_ADDRESS_MULTIGRAIN as "person_firm_address_multigrain_1" on ("root".PERSON_ADDRESS_OID = "person_firm_address_multigrain_1".OID) where "root".DLEVEL = \'P\'',
                    'a class mapping filter (~filter) over a multigrain table'),
        unsupported("testToManyWithQualifierWithFilterOnDataType", f"{_SRC}::testToManyWithQualifierWithFilterOnDataType",
                    'select "root".QTY as "num" from POSITION as "root" left outer join FIRM_ACCT_IF_MULTIGRAIN as "firm_acct_if_multigrain_0" on ("root".ACCT_I = "firm_acct_if_multigrain_0".OID) left outer join FIRM_ACCT_IF_MULTIGRAIN as "firm_acct_if_multigrain_1" on ("firm_acct_if_multigrain_0".ACCOUNT_NUM = "firm_acct_if_multigrain_1".ACCOUNT_NUM and "firm_acct_if_multigrain_1".IF_TYPE = \'P\' and "firm_acct_if_multigrain_1".DLEVEL = \'S\') where "firm_acct_if_multigrain_1".IF_NUM = 1022',
                    'a class mapping filter (~filter) over a multigrain table'),
        unsupported("testToManyWithQualifierWithFilterOnJoin", f"{_SRC}::testToManyWithQualifierWithFilterOnJoin",
                    'select "root".QTY as "num" from POSITION as "root" left outer join FIRM_ACCT_IF_MULTIGRAIN as "firm_acct_if_multigrain_0" on ("root".ACCT_I = "firm_acct_if_multigrain_0".OID) left outer join FIRM_ACCT_IF_MULTIGRAIN as "firm_acct_if_multigrain_1" on ("firm_acct_if_multigrain_0".ACCOUNT_NUM = "firm_acct_if_multigrain_1".ACCOUNT_NUM and "firm_acct_if_multigrain_1".IF_TYPE = \'P\' and "firm_acct_if_multigrain_1".DLEVEL = \'S\') left outer join IF_OTHER_INFO as "if_other_info_0" on ("firm_acct_if_multigrain_1".IF_NUM = "if_other_info_0".IF_CODE) where "if_other_info_0".IF_NAME = \'IfName1\'',
                    'a class mapping filter (~filter) over a multigrain table'),
    ],
)
