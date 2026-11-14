# Legend TDS Join With Filter Mapping

Source: legend-engine core_relational `tds/tests/testTDSJoin.pure` (`testJoinTDSMappingWithFilter`): the person of `dbInc` with its
columns mapped as properties, and the firm with the class mapping filter FirmXFilter, which is a query here: the firms named 'Firm X'.

## Model: tds_join_model.md

## DataStore: dbInc (Database)

### Schema: main

#### Table: personTable → JoinPerson

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | personID    |
| FIRSTNAME | VARCHAR |     | Y        | firstName   |
| LASTNAME  | VARCHAR |     | Y        | lastName    |
| AGE       | INT     |     | Y        | age         |
| FIRMID    | INT     |     | Y        | employerID  |
| MANAGERID | INT     |     | Y        | managerID   |

#### Table: firmTable

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        |             |
| LEGALNAME | VARCHAR |     | Y        |             |
| ADDRESSID | INT     |     | Y        |             |

#### Query: FIRM_X → JoinFirm

```sql
SELECT ID, LEGALNAME, ADDRESSID
FROM firmTable
WHERE LEGALNAME = 'Firm X'
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | firmID      |
| LEGALNAME | VARCHAR |     | Y        | legalName   |
| ADDRESSID | INT     |     | Y        | firmAddrID  |
