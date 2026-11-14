# Legend TDS Join Mapping

Source: legend-engine core_relational `tds/tests/testTDSJoin.pure` (`testJoinTDSMapping`): the person and the firm of `dbInc`, each
with its columns mapped as properties.

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

#### Table: firmTable → JoinFirm

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | firmID      |
| LEGALNAME | VARCHAR |     | Y        | legalName   |
| ADDRESSID | INT     |     | Y        | firmAddrID  |
