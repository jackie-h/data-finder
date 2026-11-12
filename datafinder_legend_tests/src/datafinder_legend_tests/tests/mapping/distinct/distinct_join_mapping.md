# Legend Distinct With Join Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMappingWithJoin`: IncomeFunction is `~distinct`
over ACCOUNT_INFO, with its IfName taken through the join `otherInfo` to IF_OTHER_INFO, so the join is inside the distinct;
Classification is IF_OTHER_INFO and reaches its income functions through the same join).

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

| Source Column | Target Table    | Target Column |
|---------------|-----------------|---------------|
| IF_CODE       | INCOME_FUNCTION | IF_CODE       |

#### Query: INCOME_FUNCTION → IncomeFunction

```sql
SELECT DISTINCT a.IF_CODE, a.IF_NAME, o.IF_OTHER_NAME
FROM ACCOUNT_INFO a
LEFT OUTER JOIN IF_OTHER_INFO o ON a.IF_CODE = o.IF_CODE
```

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| IF_CODE       | INT     | PK  | Y        | code        |
| IF_NAME       | VARCHAR |     | Y        | name        |
| IF_OTHER_NAME | VARCHAR |     | Y        | ifName      |
