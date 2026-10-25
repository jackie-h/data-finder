"""
Ported from legend-engine core_relational ``tests/query/testView.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/query/testView.pure"


VIEW_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testViewWithJoinsAndDistinct", f"{_SRC}::testViewWithJoinsAndDistinct",
                    'select "personviewwithdistinct_0".firstName as "firstname" from firmTable as "root" left outer join (select distinct "persontable_1".ID as id, "persontable_1".FIRSTNAME as firstName, "persontable_1".LASTNAME as lastName, "persontable_1".FIRMID as firmId from (select "root".ID as id, max("root".AGE) as maxage from personTable as "root" group by "root".ID) as "root" left outer join personTable as "persontable_1" on ("persontable_1".ID = "root".id and "persontable_1".AGE = "root".maxage)) as "personviewwithdistinct_0" on ("root".ID = "personviewwithdistinct_0".firmId)',
                    'no mapping through database views'),
        unsupported("testDistinctOnlyIncludesTopLevelColumns", f"{_SRC}::testDistinctOnlyIncludesTopLevelColumns",
                    'select "root".firstName as "firstName", "root".address as "address" from (select distinct "root".FIRSTNAME as firstName, "addresstable_0".NAME as address from personTable as "root" left outer join addressTable as "addresstable_0" on ("addresstable_0".ID = "root".ADDRESSID) where "root".FIRSTNAME = \'John\') as "root" where "root".firstName = \'John\'',
                    'no mapping through database views'),
        unsupported("testViewPropertyFilterWithPrimaryKey", f"{_SRC}::testViewPropertyFilterWithPrimaryKey",
                    'select "dept_2".name as "Category" from (select "root".OrgId as OrgId, "root".name as name from (select "root".OrgId as OrgId, "root".name as name from Org as "root") as "root") as "root" left outer join (select "orgviewonview_2".OrgId as OrgId, "branch_0".name as name_1, "branch_1".name as name from (select "root".OrgId as OrgId, "root".name as name from (select "root".OrgId as OrgId, "root".name as name from Org as "root") as "root") as "orgviewonview_2" left outer join Dept as "dept_0" on ("orgviewonview_2".name = "dept_0".name) inner join Branch as "branch_0" on ("dept_0".id = "branch_0".branchId) left outer join Dept as "dept_1" on ("orgviewonview_2".name = "dept_1".name) inner join Branch as "branch_1" on ("dept_1".id = "branch_1".branchId)) as "orgviewonview_1" on ("root".OrgId = "orgviewonview_1".OrgId) left outer join (select "dept_3".name as name_1, "branch_2".name as name from Dept as "dept_3" inner join Branch as "branch_2" on ("dept_3".id = "branch_2".branchId)) as "dept_2" on ("root".name = "dept_2".name_1) where case when 1 = 1 then case when "orgviewonview_1".name_1 is null then \'\' else \'A\' end else case when "orgviewonview_1".name is null then \'\' else \'B\' end end = \'A\'',
                    'no mapping through database views'),
    ],
)
