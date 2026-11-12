# Legend Distinct Full Denorm Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMappingFullDenorm`). ACCOUNT_INFO holds the
account, its income function and the income function's desk head. The income functions are its distinct rows of type 'Type 1' and
the desk heads its distinct rows, each a query; an account reaches its income function, and an income function its desk head, through
joins between them.

## Model: distinct_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ACCOUNT_INFO → FirmAccount

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| id            | INT     | PK  | N        |             |
| ACC_NUM       | INT     |     | Y        | number      |
| ACCT_NAME     | VARCHAR |     | Y        | name        |
| IF_CODE       | INT     |     | Y        |             |
| IF_NAME       | VARCHAR |     | Y        |             |
| IF_TYPE       | VARCHAR |     | Y        |             |
| DESKHEAD_ID   | INT     |     | Y        |             |
| DESKHEAD_NAME | VARCHAR |     | Y        |             |

#### Association: FirmAccountIncomeFunction

| Source Column | Target Table    | Target Column |
|---------------|-----------------|---------------|
| IF_CODE       | INCOME_FUNCTION | IF_CODE       |

#### Query: INCOME_FUNCTION → IncomeFunction

```sql
SELECT DISTINCT IF_CODE, IF_NAME, DESKHEAD_ID
FROM ACCOUNT_INFO
WHERE IF_TYPE = 'Type 1'
```

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| IF_CODE     | INT     | PK  | Y        | code        |
| IF_NAME     | VARCHAR |     | Y        | name        |
| DESKHEAD_ID | INT     |     | Y        |             |

#### Association: IncomeFunctionDeskHead

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| DESKHEAD_ID   | DESK_HEAD    | DESKHEAD_ID   |

#### Query: DESK_HEAD → DeskHead

```sql
SELECT DISTINCT DESKHEAD_NAME, DESKHEAD_ID
FROM ACCOUNT_INFO
```

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| DESKHEAD_NAME | VARCHAR |     | Y        | name        |
| DESKHEAD_ID   | INT     | PK  | Y        | code        |
