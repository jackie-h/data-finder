"""
Ported from legend-engine core_relational ``tds/tests/testTDSRestrict.pure``.

Restricting the columns of a projection or a group by. A restrict after a group by is a derived table in
Legend; here the group by selects only the columns that are kept, which returns the same rows.
Mapping: tests/simple_mapping.md.
"""
import datetime

from datafinder import constant
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported, BACKEND, JOIN_OPERAND_ORDER, RESTRICT_AFTER_GROUP_BY

_SRC = "tds/tests/testTDSRestrict.pure"

RESTRICT_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        ported("testProjectAllColumns_Single", f"{_SRC}::testProjectAllColumns_Single",
               'select "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testLowerProjectColsEliminated", f"{_SRC}::testLowerProjectColsEliminated",
               'select "root".FIRSTNAME as "firstName" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name()])),
        ported("testRestrictOnGroupByEleminatesUncessaryAggs", f"{_SRC}::testRestrictOnGroupByEleminatesUncessaryAggs",
               'select "firmtable_0".LEGALNAME as "Firm", count("root".FIRSTNAME) as "People count" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) group by "Firm" order by "Firm" asc',
               lambda f: f.find_all(None, None, [f.firm().legal_name(), f.first_name().count()])
                      .group_by(f.firm().legal_name()).order_by(f.firm().legal_name().ascending())),
        ported("testRestrictOnGroupByColumn_SubSetOfGroupByColumns", f"{_SRC}::testRestrictOnGroupByColumn_SubSetOfGroupByColumns",
               'select "persontable_0"."Firm" as "Firm", "persontable_0"."People count" as "People count" from (select "firmtable_0".LEGALNAME as "Firm", "locationtable_0".PLACE as "Place", count("root".FIRSTNAME) as "People count" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID) group by "Firm", "Place") as "persontable_0" order by "Firm" asc, "People count" asc',
               lambda f: f.find_all(None, None, [f.firm().legal_name(), f.first_name().count()])
               .group_by(f.firm().legal_name(), f.locations().place())
               .order_by(f.firm().legal_name().ascending(), f.first_name().count().ascending()),
               known_gaps={BACKEND: RESTRICT_AFTER_GROUP_BY}),
        ported("testRestrictOnGroupByColumn_DropAllAggColumns", f"{_SRC}::testRestrictOnGroupByColumn_DropAllAggColumns",
               'select "persontable_0"."Firm" as "Firm" from (select "firmtable_0".LEGALNAME as "Firm", "locationtable_0".PLACE as "Place" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) left outer join locationTable as "locationtable_0" on ("root".ID = "locationtable_0".PERSONID) group by "Firm", "Place") as "persontable_0" order by "Firm" asc',
               lambda f: f.find_all(None, None, [f.firm().legal_name()])
                      .group_by(f.firm().legal_name(), f.locations().place()).order_by(f.firm().legal_name().ascending()),
               known_gaps={BACKEND: RESTRICT_AFTER_GROUP_BY}),
        ported("testProjectAllColumns_Mulitple", f"{_SRC}::testProjectAllColumns_Mulitple",
               'select "root".FIRSTNAME as "firstName", \'Hello\' as "Hello" from personTable as "root"',
               lambda f: f.find_all(None, None, [f.first_name(), constant("Hello")])),
        unsupported("testLowerProjectColsNotEliminatedWithDistinct", f"{_SRC}::testLowerProjectColsNotEliminatedWithDistinct",
                    'select "persontable_0"."firstName" as "firstName" from (select distinct "root".FIRSTNAME as "firstName", \'Hello\' as "Hello" from personTable as "root" order by "firstName" asc) as "persontable_0"',
                    'restrict, sort, limit and distinct in sequence nest subselects, and Legend keeps joins to restricted-away columns; the finder builds one query'),
        unsupported("testLowerProjectColsNotEliminatedWithSort", f"{_SRC}::testLowerProjectColsNotEliminatedWithSort",
                    'select "persontable_0"."firstName" as "firstName" from (select "root".FIRSTNAME as "firstName", \'Hello\' as "Hello" from personTable as "root" order by "Hello" asc, "firstName" asc) as "persontable_0"',
                    'a restrict after a sort wraps the query in a subselect'),
        unsupported("testRestrictWithPostProcessor", f"{_SRC}::testRestrictWithPostProcessor",
                    'select "persontable_0"."firstName" as "firstName" from (select "root".FIRSTNAME as "firstName", \'Hello\' as "Hello" from personTable as "root" order by "Hello" asc, "firstName" asc) as "persontable_0"',
                    'a restrict after a sort wraps the query in a subselect'),
        unsupported("testRestrictOnGroupByEleminatesUnnecessaryAggsWithDistinct", f"{_SRC}::testRestrictOnGroupByEleminatesUnnecessaryAggsWithDistinct",
                    'select "persontable_0"."Firm" as "Firm", "persontable_0"."People count" as "People count" from (select distinct "firmtable_0".LEGALNAME as "Firm", count("root".FIRSTNAME) as "People count" from personTable as "root" left outer join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID) group by "Firm" order by "Firm" asc) as "persontable_0"',
                    'a mapping that the port does not load'),
    ],
)
