# Legend Union Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMapping`): Person is the union of two set
implementations, PersonSet1 and PersonSet2, and the firm and the employees are joined through each set's FirmID. The union is a
query here: the two selects joined by UNION ALL, with the FirmID of each that the joins read.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: Firm → Firm

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| ID     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        | legalName   |

#### Table: PersonSet1

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| ID           | INT     | PK  | N        |             |
| firstName_s1 | VARCHAR |     | Y        |             |
| lastName_s1  | VARCHAR |     | Y        |             |
| FirmID       | INT     |     | Y        |             |
| ADDRESSID    | INT     |     | Y        |             |

#### Table: PersonSet2

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| ID           | INT     | PK  | N        |             |
| firstName_s2 | VARCHAR |     | Y        |             |
| lastName_s2  | VARCHAR |     | Y        |             |
| FirmID       | INT     |     | Y        |             |
| ADDRESSID    | INT     |     | Y        |             |

#### Query: PERSON_UNION → Person

```sql
SELECT lastName_s1 AS lastName, FirmID FROM PersonSet1
UNION ALL
SELECT lastName_s2 AS lastName, FirmID FROM PersonSet2
```

| Column   | Type    | Key | Nullable | Property ID |
|----------|---------|-----|----------|-------------|
| lastName | VARCHAR |     | Y        | lastName    |
| FirmID   | INT     |     | Y        |             |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FirmID        | Firm         | ID            |
