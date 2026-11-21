# Legend Union With The Same Join In Both Sets Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithSameJoinInPropertyInBothUnions`).
Person is the union of two sets that read the same table, PersonMaster, and both read extraInformation and otherNames through the same join to PersonAdditional, on its key, inside the query of the set. Firm is the union of FirmSet1 and FirmSet2: the first set of Person joins the first set of Firm and the second joins the second.

## Model: union_extra_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonMaster

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y |  |
| FirmID | INT |  | Y |  |

#### Table: PersonAdditional

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| extrainfo | VARCHAR |  | Y |  |
| otherName | VARCHAR |  | Y |  |

#### Table: FirmSet1

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y |  |

#### Table: FirmSet2

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y |  |

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT m.ID, m.lastName, a.extrainfo AS extraInformation, a.otherName AS otherNames, m.FirmID
FROM PersonMaster m
LEFT OUTER JOIN PersonAdditional a ON m.ID = a.ID
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| extraInformation | VARCHAR |  | Y | extraInformation |
| otherNames | VARCHAR |  | Y | otherNames |
| FirmID | INT |  | Y |  |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT m.ID, m.lastName, a.extrainfo AS extraInformation, a.otherName AS otherNames, m.FirmID
FROM PersonMaster m
LEFT OUTER JOIN PersonAdditional a ON m.ID = a.ID
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| extraInformation | VARCHAR |  | Y | extraInformation |
| otherNames | VARCHAR |  | Y | otherNames |
| FirmID | INT |  | Y |  |

#### Query: FIRM_SET1 → Firm (set: FirmSet1)

```sql
SELECT id, name FROM FirmSet1
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

#### Query: FIRM_SET2 → Firm (set: FirmSet2)

```sql
SELECT id, name FROM FirmSet2
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

#### Association: PersonFirm (source set: set1, target set: FirmSet1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET1 | id |

#### Association: PersonFirm (source set: set2, target set: FirmSet2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET2 | id |
