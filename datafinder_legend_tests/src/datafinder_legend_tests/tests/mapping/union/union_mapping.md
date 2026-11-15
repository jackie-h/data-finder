# Legend Union Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMapping`).
Person is the union of two set implementations, PersonSet1 and PersonSet2, a set each; its firm is the table Firm, joined through each set's FirmID, a section for each set.

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
SELECT ID, lastName_s2 AS lastName, FirmID FROM PersonSet2
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
