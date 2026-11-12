# Legend Distinct Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMapping`: IncomeFunction is `~distinct` over
ACCOUNT_INFO, its code and name).

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
```

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| IF_CODE | INT     | PK  | Y        | code        |
| IF_NAME | VARCHAR |     | Y        | name        |
