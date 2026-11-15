# Legend Union With An Embedded Property That Is A Constant

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithEmbeddedPropertyWithConstant`).
Person is the union of two sets, a query for each of PersonFirmSet1 and PersonFirmSet2, and its firm is not a table: the firm's legal name is embedded in the row of each set, and is a constant: 'Wendys' in the first and 'Vogue' in the second.

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

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT ID, lastName_s1 AS lastName, 'Wendys' AS firm_legal_name FROM PersonFirmSet1
```

| Column          | Type    | Key | Nullable | Property ID    |
|-----------------|---------|-----|----------|----------------|
| ID              | INT     | PK  | N        | id             |
| lastName        | VARCHAR |     | Y        | lastName       |
| firm_legal_name | VARCHAR |     | Y        | firm.legalName |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT ID, lastName_s2 AS lastName, 'Vogue' AS firm_legal_name FROM PersonFirmSet2
```

| Column          | Type    | Key | Nullable | Property ID    |
|-----------------|---------|-----|----------|----------------|
| ID              | INT     | PK  | N        | id             |
| lastName        | VARCHAR |     | Y        | lastName       |
| firm_legal_name | VARCHAR |     | Y        | firm.legalName |
