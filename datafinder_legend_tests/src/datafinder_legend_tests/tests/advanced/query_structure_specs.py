"""
Ported from legend-engine core_relational ``tests/advanced/testQueryStructure.pure``.

Query structure with nested filters.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/advanced/testQueryStructure.pure"

QUERY_STRUCTURE_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testQualifierWithLineAndOrWithInLine", f"{_SRC}::testQualifierWithLineAndOrWithInLine",
                    'select "root".LEGALNAME as "name1", "persontable_0".FIRSTNAME as "name2", "persontable_0".LASTNAME as "name3" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FIRSTNAME as FIRSTNAME, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) where "persontable_1".LASTNAME = \'Smith\' or "addresstable_0".NAME = \'New York\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID) where "root".LEGALNAME = \'Firm X\'',
                    'no filter inside a navigation'),
        unsupported("testQualifierWithForkAndOrWithInline", f"{_SRC}::testQualifierWithForkAndOrWithInline",
                    'select "persontable_3".FIRSTNAME as "name1", "persontable_3".LASTNAME as "name2", "persontable_0".LASTNAME as "name3", "persontable_3".LASTNAME as "name4", "persontable_4".LASTNAME as "name5" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) left outer join personTable as "persontable_2" on ("persontable_1".MANAGERID = "persontable_2".ID) where "addresstable_0".NAME = \'Bla\' or concat("persontable_2".FIRSTNAME, \' \', "persontable_2".LASTNAME) = \'John Johnson\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".FIRSTNAME as FIRSTNAME, "persontable_1".LASTNAME as LASTNAME from personTable as "persontable_1" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "persontable_1".ADDRESSID) left outer join personTable as "persontable_2" on ("persontable_1".MANAGERID = "persontable_2".ID) where "addresstable_0".NAME = \'Hoboken\' or concat("persontable_2".FIRSTNAME, \' \', "persontable_2".LASTNAME) = \'Bla\') as "persontable_3" on ("root".ID = "persontable_3".FIRMID) left outer join (select "persontable_5".FIRMID as FIRMID, "persontable_5".LASTNAME as LASTNAME from personTable as "persontable_5" left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "persontable_5".ADDRESSID) left outer join personTable as "persontable_6" on ("persontable_5".MANAGERID = "persontable_6".ID) where "addresstable_1".NAME = \'test\' or concat("persontable_6".FIRSTNAME, \' \', "persontable_6".LASTNAME) = \'John Johnson\') as "persontable_4" on ("root".ID = "persontable_4".FIRMID) where "root".LEGALNAME = \'Firm X\'',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithIsolation", f"{_SRC}::testQualifierWithIsolation",
                    'select case when "persontable_0".LASTNAME = \'ok\' then null else "persontable_0".AGE * "persontable_4".AGE end as "name1" from firmTable as "root" left outer join (select "persontable_1".FIRMID as FIRMID, "persontable_1".LASTNAME as LASTNAME, "persontable_1".AGE as AGE from personTable as "persontable_1" where "persontable_1".LASTNAME = \'Smith\') as "persontable_0" on ("root".ID = "persontable_0".FIRMID) left outer join (select "persontable_3".FIRMID as FIRMID from personTable as "persontable_3" where "persontable_3".LASTNAME = \'Smith2\') as "persontable_2" on ("root".ID = "persontable_2".FIRMID) left outer join firmTable as "firmtable_1" on ("firmtable_1".ID = "persontable_2".FIRMID) left outer join (select "persontable_5".FIRMID as FIRMID, "persontable_5".AGE as AGE from personTable as "persontable_5" where "persontable_5".LASTNAME = \'Smith3\') as "persontable_4" on ("firmtable_1".ID = "persontable_4".FIRMID)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithIsolationXX", f"{_SRC}::testQualifierWithIsolationXX",
                    'select case when "persontable_0".LASTNAME = \'ok\' then null else concat("persontable_0".LASTNAME, "firmtable_1".LEGALNAME) end as "name1" from firmTable as "root" left outer join personTable as "persontable_0" on ("root".ID = "persontable_0".FIRMID and "persontable_0".LASTNAME = \'Smith\') left outer join firmTable as "firmtable_1" on ("firmtable_1".ID = "persontable_0".FIRMID)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithIsolationForced", f"{_SRC}::testQualifierWithIsolationForced",
                    'select case when "persontable_1".LASTNAME = \'smith\' then \'ok\' else \'bla\' end as "name1", "firmtable_1".LEGALNAME as "name3" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join (select "persontable_2".FIRMID as FIRMID, "persontable_2".LASTNAME as LASTNAME from personTable as "persontable_2" where "persontable_2".AGE < 30) as "persontable_1" on ("firmtable_0".ID = "persontable_1".FIRMID) left outer join firmTable as "firmtable_1" on ("firmtable_1".ID = "persontable_1".FIRMID)',
                    'no qualified (derived) properties'),
        unsupported("testQualifierWithIsolationForced2", f"{_SRC}::testQualifierWithIsolationForced2",
                    'select "firmtable_1".LEGALNAME as "name1", "addresstable_0".NAME as "name2" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join (select "persontable_2".FIRMID as FIRMID from personTable as "persontable_2" where "persontable_2".AGE < 30) as "persontable_1" on ("firmtable_0".ID = "persontable_1".FIRMID) left outer join firmTable as "firmtable_1" on ("firmtable_1".ID = "persontable_1".FIRMID) left outer join (select "persontable_4".FIRMID as FIRMID from personTable as "persontable_4" where "persontable_4".AGE < 20) as "persontable_3" on ("firmtable_0".ID = "persontable_3".FIRMID) left outer join firmTable as "firmtable_2" on ("firmtable_2".ID = "persontable_3".FIRMID) left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "firmtable_2".ADDRESSID)',
                    'no qualified (derived) properties'),
    ],
)
