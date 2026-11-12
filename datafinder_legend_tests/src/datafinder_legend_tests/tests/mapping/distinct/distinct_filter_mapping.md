# Legend Distinct With Filter Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMappingWithFilter`: IncomeFunction is
`~filter typeFilter ~distinct` over ACCOUNT_INFO).

## Model: distinct_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ACCOUNT_INFO

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| id            | INT     | PK  | N        |             |
| ACC_NUM       | INT     |     | Y        |             |
| ACCT_NAME     | VARCHAR |     | Y        |             |
| IF_CODE       | INT     |     | Y        |             |
| IF_NAME       | VARCHAR |     | Y        |             |
| IF_TYPE       | VARCHAR |     | Y        |             |
| DESKHEAD_ID   | INT     |     | Y        |             |
| DESKHEAD_NAME | VARCHAR |     | Y        |             |

#### Query: INCOME_FUNCTION → IncomeFunction

```sql
SELECT DISTINCT IF_CODE, IF_NAME
FROM ACCOUNT_INFO
WHERE IF_TYPE = 'Type 1'
```

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| IF_CODE | INT     | PK  | Y        | code        |
| IF_NAME | VARCHAR |     | Y        | name        |
