# Legend Union To Union Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionToUnionMapping`).
Person is the union of PersonSet1 and PersonSet2, and Firm the union of FirmSet1 and FirmSet2: a set for each. A person's firm is in either of the firm sets, so each set of Person joins each set of Firm, a section for each of the four pairs.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonSet1

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| firstName_s1 | VARCHAR |  | Y |  |
| lastName_s1 | VARCHAR |  | Y |  |
| FirmID | INT |  | Y |  |
| ADDRESSID | INT |  | Y |  |
| NICKNAME | VARCHAR |  | Y |  |

#### Table: PersonSet2

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| firstName_s2 | VARCHAR |  | Y |  |
| lastName_s2 | VARCHAR |  | Y |  |
| FirmID | INT |  | Y |  |
| ADDRESSID | INT |  | Y |  |

#### Table: FirmSet1

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y |  |
| NICKNAME | VARCHAR |  | Y |  |

#### Table: FirmSet2

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y |  |

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT ID, lastName_s1 AS lastName, FirmID FROM PersonSet1
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| FirmID | INT |  | Y |  |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT ID, lastName_s2 AS lastName, FirmID FROM PersonSet2
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
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

#### Association: PersonFirm (source set: set1, target set: firm_set1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET1 | id |

#### Association: PersonFirm (source set: set1, target set: firm_set2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET2 | id |

#### Association: PersonFirm (source set: set2, target set: firm_set1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET1 | id |

#### Association: PersonFirm (source set: set2, target set: firm_set2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | FIRM_SET2 | id |
