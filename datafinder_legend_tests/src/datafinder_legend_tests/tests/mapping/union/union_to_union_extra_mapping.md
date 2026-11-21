# Legend Union To Union Mapping With Extra Information

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionToUnionMapping2`).
Person is the one set PersonSet1, whose extraInformation is read through a join to PersonAdditional, on its key, inside the query of the set. Firm is the union of FirmSet1 and FirmSet2: a set for each, and the person's firm is in either of them, so the set of Person joins each set of Firm.

## Model: union_extra_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonSet1

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName_s1 | VARCHAR |  | Y |  |
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

#### Query: PERSON → Person

```sql
SELECT s.ID, s.lastName_s1 AS lastName, a.extrainfo AS extraInformation, s.FirmID
FROM PersonSet1 s
LEFT OUTER JOIN PersonAdditional a ON s.ID = a.ID
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| extraInformation | VARCHAR |  | Y | extraInformation |
| FirmID | INT |  | Y |  |

#### Query: FIRM_SET1 → Firm (set: firm_set1)

```sql
SELECT id, name FROM FirmSet1
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

#### Query: FIRM_SET2 → Firm (set: firm_set2)

```sql
SELECT id, name FROM FirmSet2
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

#### Association: PersonFirm (target set: firm_set1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET1 | id |

#### Association: PersonFirm (target set: firm_set2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET2 | id |
