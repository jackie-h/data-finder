"""
Ported from legend-engine core_relational ``functions/tests/testConcatenate.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "functions/tests/testConcatenate.pure"


CONCATENATE_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("testConcatenateWithFilter", f"{_SRC}::testConcatenateWithFilter",
                    'select "root".NAME as "a", "unionalias_0"."name" as "b" from productSchema.productTable as "root" left outer join (select "producttable_1".NAME as "name", "producttable_1".ID as ID from productSchema.productTable as "producttable_1" where "producttable_1".NAME like \'Firm X%\' union all select "producttable_1".NAME as "name", "producttable_1".ID as ID from productSchema.productTable as "producttable_1" where "producttable_1".NAME like \'Firm C%\') as "unionalias_0" on ("root".ID = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateClass", f"{_SRC}::testConcatenateClass",
                    'select "root".ID as "pk_0", "root".NAME as "name" from productSchema.productTable as "root" left outer join (select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'ISIN\' union all select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'CUSIP\') as "unionalias_0" on ("unionalias_0".PRODID = "root".ID) where "unionalias_0"."synonymTableNAME" = \'ISIN2\'',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateClassMerge", f"{_SRC}::testConcatenateClassMerge",
                    'select "root".NAME as "prod", "unionalias_0"."synonymTableNAME" as "syn", "unionalias_0"."synonymTableTYPE" as "type" from productSchema.productTable as "root" left outer join (select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME", "synonymtable_0".TYPE as "synonymTableTYPE" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'ISIN\' union all select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME", "synonymtable_0".TYPE as "synonymTableTYPE" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'CUSIP\') as "unionalias_0" on ("unionalias_0".PRODID = "root".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateClassAgg", f"{_SRC}::testConcatenateClassAgg",
                    'select "root".NAME as "prod", "producttable_1".aggCol as "syn" from productSchema.productTable as "root" left outer join (select "producttable_2".ID as ID, listagg("unionalias_0"."synonymTableNAME", \'|\') as aggCol from productSchema.productTable as "producttable_2" left outer join (select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'ISIN\' union all select "synonymtable_0".PRODID as PRODID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.synonymTable as "synonymtable_0" where "synonymtable_0".TYPE = \'CUSIP\') as "unionalias_0" on ("unionalias_0".PRODID = "producttable_2".ID) group by "producttable_2".ID) as "producttable_1" on ("root".ID = "producttable_1".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateClassJoin", f"{_SRC}::testConcatenateClassJoin",
                    'select "root".ID as "trade", "unionalias_0"."synonymTableNAME" as "syn" from tradeTable as "root" left outer join (select "producttable_0".ID as ID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.productTable as "producttable_0" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "producttable_0".ID) where "synonymtable_0".TYPE = \'ISIN\' union all select "producttable_0".ID as ID, "synonymtable_0".NAME as "synonymTableNAME" from productSchema.productTable as "producttable_0" left outer join productSchema.synonymTable as "synonymtable_0" on ("synonymtable_0".PRODID = "producttable_0".ID) where "synonymtable_0".TYPE = \'CUSIP\') as "unionalias_0" on ("root".prodId = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateInQualifierWithComplexReturnType", f"{_SRC}::testConcatenateInQualifierWithComplexReturnType",
                    'select concat("root".FIRSTNAME, \' \', "root".LASTNAME) as "name", "unionalias_0"."addressTableNAME" as "address" from personTable as "root" left outer join (select "addresstable_0".ID as ID, null as ADDRESSID, "addresstable_0".NAME as "addressTableNAME" from addressTable as "addresstable_0" union all select "firmtable_0".ID as ID, "firmtable_0".ADDRESSID as ADDRESSID, "addresstable_1".NAME as "addressTableNAME" from firmTable as "firmtable_0" left outer join addressTable as "addresstable_1" on ("addresstable_1".ID = "firmtable_0".ADDRESSID)) as "unionalias_0" on ("unionalias_0".ID = "root".FIRMID or "unionalias_0".ID = "root".ADDRESSID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testQualifierConcatenateTwoSimilarJoins", f"{_SRC}::testQualifierConcatenateTwoSimilarJoins",
                    'select "root".ID as "Trade ID", "unionalias_0"."OENAME" as "OE" from CONCATENATE.TRADE as "root" left outer join (select "subaccount_0".ID as ID, "subaccount_0".OE_ID as OE_ID, null as EA_ID, null as EA_OE_ID, "oe_0".NAME as "OENAME" from CONCATENATE.SUBACCOUNT as "subaccount_0" left outer join CONCATENATE.OE as "oe_0" on ("subaccount_0".OE_ID = "oe_0".ID) union all select null as ID, null as OE_ID, "externalaccount_0".EA_ID as EA_ID, "externalaccount_0".EA_OE_ID as EA_OE_ID, "oe_0".NAME as "OENAME" from CONCATENATE.EXTERNALACCOUNT as "externalaccount_0" left outer join CONCATENATE.OE as "oe_0" on ("externalaccount_0".EA_OE_ID = "oe_0".ID)) as "unionalias_0" on ("root".EXTERNALACCOUNT_ID = "unionalias_0".EA_ID or "root".SUBACCOUNT_ID = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testQualifierConcatenateTwoSimilarJoinsEmbedded", f"{_SRC}::testQualifierConcatenateTwoSimilarJoinsEmbedded",
                    'select "root".ID as "Trade ID", "unionalias_0"."SUBACCOUNTOE_NAME_EXTERNALACCOUNTEA_OE_NAME" as "OE" from CONCATENATE.TRADE as "root" left outer join (select "subaccount_0".ID as ID, null as EA_ID, "subaccount_0".OE_NAME as "SUBACCOUNTOE_NAME_EXTERNALACCOUNTEA_OE_NAME" from CONCATENATE.SUBACCOUNT as "subaccount_0" union all select null as ID, "externalaccount_0".EA_ID as EA_ID, "externalaccount_0".EA_OE_NAME as "SUBACCOUNTOE_NAME_EXTERNALACCOUNTEA_OE_NAME" from CONCATENATE.EXTERNALACCOUNT as "externalaccount_0") as "unionalias_0" on ("root".EXTERNALACCOUNT_ID = "unionalias_0".EA_ID or "root".SUBACCOUNT_ID = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateFlat", f"{_SRC}::testConcatenateFlat",
                    'select "unionalias_0"."id_plus_TRADEID_18" as "Concatenated" from CONCATENATE.TRADE as "root" left outer join (select "trade_1".ID as "id_plus_TRADEID_18", "trade_1".ID as ID from CONCATENATE.TRADE as "trade_1" union all select "trade_1".ID + 18 as "id_plus_TRADEID_18", "trade_1".ID as ID from CONCATENATE.TRADE as "trade_1") as "unionalias_0" on ("root".ID = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
        unsupported("testConcatenateFlatWithOtherProperty", f"{_SRC}::testConcatenateFlatWithOtherProperty",
                    'select "root".ID as "simple", "unionalias_0"."id_plus_TRADEID_18" as "Concatenated" from CONCATENATE.TRADE as "root" left outer join (select "trade_1".ID as "id_plus_TRADEID_18", "trade_1".ID as ID from CONCATENATE.TRADE as "trade_1" union all select "trade_1".ID + 18 as "id_plus_TRADEID_18", "trade_1".ID as ID from CONCATENATE.TRADE as "trade_1") as "unionalias_0" on ("root".ID = "unionalias_0".ID)',
                    'concatenating the values of a property inside a projection (a derived table joined to the root)'),
    ],
)
