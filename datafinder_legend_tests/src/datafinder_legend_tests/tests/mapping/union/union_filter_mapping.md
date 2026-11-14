# Legend Union With Filter Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithFilter`): Person is the union of two
set implementations, PersonSet1 and PersonSet2, each with a class mapping filter (FirmID = 1). The union is a query here: the two
selects joined by UNION ALL, each with its filter as its WHERE.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonSet1

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| ID           | INT     | PK  | N        |             |
| firstName_s1 | VARCHAR |     | Y        |             |
| lastName_s1  | VARCHAR |     | Y        |             |
| FirmID       | INT     |     | Y        |             |
| ADDRESSID    | INT     |     | Y        |             |
| NICKNAME     | VARCHAR |     | Y        |             |

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
SELECT lastName_s1 AS lastName FROM PersonSet1 WHERE FirmID = 1
UNION ALL
SELECT lastName_s2 AS lastName FROM PersonSet2 WHERE FirmID = 1
```

| Column   | Type    | Key | Nullable | Property ID |
|----------|---------|-----|----------|-------------|
| lastName | VARCHAR |     | Y        | lastName    |
