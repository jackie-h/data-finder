# Legend Distinct With Join And Condition Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMappingWithJoinWithDup`: IncomeFunction is `~distinct`
over ACCOUNT_INFO, with its IfName taken through the join `otherInfo2`, which has `IF_CODE is not null` besides the key to IF_OTHER_INFO, so the join is inside the distinct;
Classification is IF_OTHER_INFO and reaches its income functions through the same join). Only the name is mapped, as in Legend.

## Model: distinct_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ACCOUNT_INFO

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| id            | INT     | PK  | N        |             |
| IF_CODE       | INT     |     | Y        |             |
| IF_NAME       | VARCHAR |     | Y        |             |

#### Table: IF_OTHER_INFO → Classification

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| IF_CODE       | INT     | PK  | N        |             |
| IF_OTHER_NAME | VARCHAR |     | Y        | name        |

#### Association: ClassificationIncomeFunctions

| Source Column | Target Table    | Target Column | Condition                           |
|---------------|-----------------|---------------|-------------------------------------|
| IF_CODE       | INCOME_FUNCTION | IF_CODE       | INCOME_FUNCTION.IF_CODE IS NOT NULL |

#### Query: INCOME_FUNCTION → IncomeFunction

```sql
SELECT DISTINCT a.IF_NAME, o.IF_OTHER_NAME, a.IF_CODE
FROM ACCOUNT_INFO a
LEFT OUTER JOIN IF_OTHER_INFO o ON a.IF_CODE = o.IF_CODE AND a.IF_CODE IS NOT NULL
```

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| IF_NAME       | VARCHAR |     | Y        | name        |
| IF_OTHER_NAME | VARCHAR |     | Y        | ifName      |
| IF_CODE       | INT     | PK  | Y        |             |
