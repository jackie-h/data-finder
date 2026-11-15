# Legend Union With An Embedded Property Read Through A Join

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithEmbeddedPropertyOneThroughJoin`).
Person is the union of two sets, a query for each of PersonFirmSet1 and PersonFirmSet2, and its firm is not a table: the firm's legal name is embedded in the row of each set, from
a join to PersonFirmSet1Firm in the first set, which the query of the set makes, and the column Firm2 of the second.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonFirmSet1

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| ID          | INT     | PK  | N        |             |
| lastName_s1 | VARCHAR |     | Y        |             |
| Firm1       | VARCHAR |     | Y        |             |

#### Table: PersonFirmSet2

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| ID          | INT     | PK  | N        |             |
| lastName_s2 | VARCHAR |     | Y        |             |
| Firm2       | VARCHAR |     | Y        |             |

#### Table: PersonFirmSet1Firm

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| FirmJ1 | VARCHAR |     | Y        |             |

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT p.ID, p.lastName_s1 AS lastName, f.FirmJ1 AS firm_legal_name
FROM PersonFirmSet1 p
LEFT JOIN PersonFirmSet1Firm f ON f.id = p.ID
```

| Column          | Type    | Key | Nullable | Property ID    |
|-----------------|---------|-----|----------|----------------|
| ID              | INT     | PK  | N        | id             |
| lastName        | VARCHAR |     | Y        | lastName       |
| firm_legal_name | VARCHAR |     | Y        | firm.legalName |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT ID, lastName_s2 AS lastName, Firm2 AS firm_legal_name FROM PersonFirmSet2
```

| Column          | Type    | Key | Nullable | Property ID    |
|-----------------|---------|-----|----------|----------------|
| ID              | INT     | PK  | N        | id             |
| lastName        | VARCHAR |     | Y        | lastName       |
| firm_legal_name | VARCHAR |     | Y        | firm.legalName |
