# Legend Class Mapping Filter With Inner Join Mapping: Target View

Source: legend-engine core_relational `tests/mapping/classMappingFilterWithInnerJoin/testRelationalSetUp.pure` (`testTableToViewMapping`).
Person is the persons of personTable whose age is the greatest for their id and under 30: personTable inner joined to a query that groups it by id, `personViewWithGroupBy`.

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
| MANAGERID | INT     |     | Y        |             |

#### Query: PERSON_VIEW_WITH_GROUP_BY

```sql
SELECT ID AS id, MAX(AGE) AS maxage
FROM personTable
GROUP BY ID
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        |             |
| maxage | INT  |     | Y        |             |

#### Query: YOUNG_PERSON → Person

```sql
SELECT p.ID, p.FIRSTNAME, p.LASTNAME, p.AGE, p.ADDRESSID, p.FIRMID, p.MANAGERID
FROM personTable p
INNER JOIN PERSON_VIEW_WITH_GROUP_BY g ON p.ID = g.id AND p.AGE = g.maxage
WHERE g.maxage < 30
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| FIRSTNAME | VARCHAR |     | Y        | firstName   |
| LASTNAME  | VARCHAR |     | Y        |             |
| AGE       | INT     |     | Y        | age         |
| ADDRESSID | INT     |     | Y        |             |
| FIRMID    | INT     |     | Y        |             |
| MANAGERID | INT     |     | Y        |             |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FIRMID        | firmTable    | ID            |
