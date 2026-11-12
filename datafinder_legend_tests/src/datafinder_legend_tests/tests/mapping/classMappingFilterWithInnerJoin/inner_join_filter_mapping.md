# Legend Class Mapping Filter With Inner Join Mapping

Source: legend-engine core_relational `tests/mapping/classMappingFilterWithInnerJoin/testRelationalSetUp.pure`
(`classOwnedMappingWithInnerJoinInFilter`: Person is `~filter (INNER) @Firm_Person | FirmXFilter` over personTable, so its rows are the
persons of the firm 'Firm X'). The persons are a query here: personTable inner joined to firmTable, where the firm's legal name is 'Firm X'.

## Model: ../../simple_model.md

## DataStore: dbInc (Database)

### Schema: main

#### Table: firmTable → Firm

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| LEGALNAME | VARCHAR |     | Y        | legalName   |

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

#### Query: FIRM_X_PERSON → Person

```sql
SELECT p.ID, p.FIRSTNAME, p.LASTNAME, p.AGE, p.ADDRESSID, p.FIRMID, p.MANAGERID
FROM personTable p
INNER JOIN firmTable f ON f.ID = p.FIRMID
WHERE f.LEGALNAME = 'Firm X'
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
