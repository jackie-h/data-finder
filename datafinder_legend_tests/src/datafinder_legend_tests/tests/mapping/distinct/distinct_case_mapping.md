# Legend Distinct With Case Mapping

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (`testMappingWithCase`). The income functions are
distinct rows of ACCOUNT_INFO, whose name is a CASE that gives 'C' for a null; Classification is IF_OTHER_INFO, whose name is a CASE
that gives 'Unknown' for a null. Each is a query here, as the CASE is part of what it selects.

## Model: distinct_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ACCOUNT_INFO

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| id            | INT     | PK  | N        |             |
| IF_CODE       | INT     |     | Y        |             |
| IF_NAME       | VARCHAR |     | Y        |             |

#### Table: IF_OTHER_INFO

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| IF_CODE       | INT     | PK  | N        |             |
| IF_OTHER_NAME | VARCHAR |     | Y        |             |

#### Query: INCOME_FUNCTION → IncomeFunction

```sql
SELECT DISTINCT IF_CODE, CASE WHEN IF_NAME IS NULL THEN 'C' ELSE IF_NAME END AS name
FROM ACCOUNT_INFO
```

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| IF_CODE | INT     | PK  | Y        | code        |
| name    | VARCHAR |     | Y        | name        |

#### Query: CLASSIFICATION → Classification

```sql
SELECT IF_CODE, CASE WHEN IF_OTHER_NAME IS NULL THEN 'Unknown' ELSE IF_OTHER_NAME END AS name
FROM IF_OTHER_INFO
```

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| IF_CODE | INT     | PK  | N        |             |
| name    | VARCHAR |     | Y        | name        |

#### Association: ClassificationIncomeFunctions

| Source Column | Target Table    | Target Column |
|---------------|----------------|---------------|
| IF_CODE       | INCOME_FUNCTION | IF_CODE      |
