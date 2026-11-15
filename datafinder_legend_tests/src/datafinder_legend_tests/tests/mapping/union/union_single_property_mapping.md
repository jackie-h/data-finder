# Legend Union With A Single Property Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionWithSinglePropertyMapping`).
Person is the union of two sets that both extend one base mapping, so both read PersonSet1: each person is there twice. Its firm is the table Firm, joined through FirmID, a section for each set.

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

#### Table: Firm → Firm

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

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
SELECT ID, lastName_s1 AS lastName, FirmID FROM PersonSet1
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| FirmID | INT |  | Y |  |

#### Association: PersonFirm (source set: set1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | Firm | ID |

#### Association: PersonFirm (source set: set2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | Firm | ID |
