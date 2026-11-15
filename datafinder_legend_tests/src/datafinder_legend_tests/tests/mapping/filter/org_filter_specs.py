"""
Ported from legend-engine core_relational ``tests/mapping/filter/testFilterMappingTree.pure``.

A class mapped with ``~filter`` is a ``Query`` section here (a ``WHERE``, with the join of a filter through a join as an ``INNER JOIN``).
Legend writes the filter into the select that reads the class, and into the ``ON`` of the joins to its parent; the finder reads the query
as a derived table, so the SQL has a level of select of its own, with the same rows.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import BACKEND, CLASS_FILTER_AS_QUERY, ported

_SRC = "tests/mapping/filter/testFilterMappingTree.pure"


ORG_FILTER_SPECS = FinderSpec(
    finder_name="OrgFinder",
    mapping_file="tests/mapping/filter/org_filter_mapping.md",
    expectations=[
        ported("testFilterMapping", f"{_SRC}::testFilterMapping",
               'select "root".id as "pk_0", "root".name as "name" from orgTable as "root" where "root".filterVal <= 4',
               lambda f: f.find_all([f.id_(), f.name()]),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
        ported("testFilterMappingWithProjection", f"{_SRC}::testFilterMappingWithProjection",
               'select "root".name as "name" from orgTable as "root" where "root".filterVal <= 4',
               lambda f: f.find_all([f.name()]),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
        ported("testFilterMappingWithProjectionOverlapp", f"{_SRC}::testFilterMappingWithProjectionOverlapp",
               'select "root".name as "name", "orgtable_1".name as "p_name", "orgtable_2".name as "p_p_name" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id and "orgtable_1".filterVal <= 4) left outer join (select "orgtable_3".id as id, "orgtable_3".name as name from orgTable as "orgtable_3" where "orgtable_3".filterVal <= 4) as "orgtable_2" on ("orgtable_1".parentId = "orgtable_2".id) where "root".filterVal <= 4',
               lambda f: f.find_all([f.name(), f.parent().name().alias("p_name"), f.parent().parent().name().alias("p_p_name")]),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
        ported("testFilterMappingQuery", f"{_SRC}::testFilterMappingQuery",
               'select "root".id as "pk_0", "root".name as "name" from orgTable as "root" left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id) where "orgtable_1".filterVal <= 4 and "root".filterVal <= 4 and "orgtable_1".name = \'Firm X\'',
               lambda f: f.find_all([f.id_(), f.name()], f.parent().name().eq("Firm X")),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
    ],
)


ORG_FILTER_JOIN_SPECS = FinderSpec(
    finder_name="OrgFinder",
    mapping_file="tests/mapping/filter/org_filter_join_mapping.md",
    expectations=[
        ported("testFilterMappingWithJoin", f"{_SRC}::testFilterMappingWithJoin",
               'select "root".id as "pk_0", "root".name as "name" from orgTable as "root" left outer join otherTable as "othertable_0" on ("root".id = "othertable_0".orgTableId) where "othertable_0".filterVal <= 4',
               lambda f: f.find_all([f.id_(), f.name()]),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
        ported("testFilterMappingWithProjectionAndJoin", f"{_SRC}::testFilterMappingWithProjectionAndJoin",
               'select "root".name as "name" from orgTable as "root" left outer join otherTable as "othertable_0" on ("root".id = "othertable_0".orgTableId) where "othertable_0".filterVal <= 4',
               lambda f: f.find_all([f.name()]),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
        ported("testFilterMappingWithProjectionAndJoinAndQuery", f"{_SRC}::testFilterMappingWithProjectionAndJoinAndQuery",
               'select "root".id as "pk_0", "root".name as "name" from orgTable as "root" left outer join otherTable as "othertable_0" on ("root".id = "othertable_0".orgTableId) left outer join orgTable as "orgtable_1" on ("root".parentId = "orgtable_1".id) left outer join otherTable as "othertable_1" on ("orgtable_1".id = "othertable_1".orgTableId) where "othertable_1".filterVal <= 4 and "othertable_0".filterVal <= 4 and "orgtable_1".name = \'Firm X\'',
               lambda f: f.find_all([f.id_(), f.name()], f.parent().name().eq("Firm X")),
               known_gaps={BACKEND: CLASS_FILTER_AS_QUERY}),
    ],
)
