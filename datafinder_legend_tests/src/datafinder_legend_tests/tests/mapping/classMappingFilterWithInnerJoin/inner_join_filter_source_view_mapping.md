# Legend Class Mapping Filter With Inner Join Mapping: Source View

Source: legend-engine core_relational `tests/mapping/classMappingFilterWithInnerJoin/testRelationalSetUp.pure` (`testViewToTableMapping`).
Person is a view of personTable, a query; its rows are those of the persons whose address is 'New York', a second query that reads the first and inner joins addressTable.

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

#### Query: NEW_YORK_PERSON → Person

```sql
SELECT v.PERSON_ID, v.firstName, v.lastName, v.age, v.firmID, v.addressID
FROM MY_PERSON_VIEW v
INNER JOIN addressTable a ON a.ID = v.addressID
WHERE a.NAME = 'New York'
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
