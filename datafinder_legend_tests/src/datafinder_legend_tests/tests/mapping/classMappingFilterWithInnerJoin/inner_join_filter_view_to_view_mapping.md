# Legend Class Mapping Filter With Inner Join Mapping: View To View

Source: legend-engine core_relational `tests/mapping/classMappingFilterWithInnerJoin/testRelationalSetUp.pure` (`testViewToViewMapping`).
Person is a view of personTable, a query; its rows are those of the persons of 'Firm C', a view of firmTable, another query, inner joined to the first.

## Model: ../../simple_model.md

## DataStore: dbInc (Database)

### Schema: main

#### Table: firmTable → Firm

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| LEGALNAME | VARCHAR |     | Y        | legalName   |
| ADDRESSID | INT     |     | Y        |             |
| CEOID     | INT     |     | Y        |             |

#### Table: addressTable

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| ID     | INT     | PK  | N        |             |
| NAME   | VARCHAR |     | Y        |             |

#### Table: personTable

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        |             |
| FIRSTNAME | VARCHAR |     | Y        |             |
| LASTNAME  | VARCHAR |     | Y        |             |
| AGE       | INT     |     | Y        |             |
| ADDRESSID | INT     |     | Y        |             |
| FIRMID    | INT     |     | Y        |             |

#### Query: MY_PERSON_VIEW

```sql
SELECT ID AS PERSON_ID, FIRSTNAME AS firstName, LASTNAME AS lastName, AGE AS age, FIRMID AS firmID, ADDRESSID AS addressID
FROM personTable
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| PERSON_ID | INT     | PK  | N        |             |
| firstName | VARCHAR |     | Y        |             |
| lastName  | VARCHAR |     | Y        |             |
| age       | INT     |     | Y        |             |
| firmID    | INT     |     | Y        |             |
| addressID | INT     |     | Y        |             |

#### Query: MY_FIRM_VIEW

```sql
SELECT ID AS FIRM_ID, LEGALNAME AS legalName, CEOID AS CEO_ID, ADDRESSID AS addressID
FROM firmTable
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| FIRM_ID   | INT     | PK  | N        |             |
| legalName | VARCHAR |     | Y        |             |
| CEO_ID    | INT     |     | Y        |             |
| addressID | INT     |     | Y        |             |

#### Query: FIRM_C_PERSON → Person

```sql
SELECT v.PERSON_ID, v.firstName, v.lastName, v.age, v.firmID, v.addressID
FROM MY_PERSON_VIEW v
INNER JOIN MY_FIRM_VIEW f ON f.FIRM_ID = v.firmID
WHERE f.legalName = 'Firm C'
```

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| PERSON_ID  | INT     | PK  | N        | id          |
| firstName  | VARCHAR |     | Y        | firstName   |
| lastName   | VARCHAR |     | Y        |             |
| age        | INT     |     | Y        |             |
| firmID     | INT     |     | Y        |             |
| addressID  | INT     |     | Y        |             |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| firmID        | firmTable    | ID            |
