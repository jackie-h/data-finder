# SQL Query Sets Mapping

A class that is the union of several tables is mapped to a query for each, and each query is a set with a name of its own: `(set: retail)`. A query is one SELECT,
never a UNION: the mapping puts the sets together as a union, gives each row the name of its set (the column `_set`), and checks that the sets agree and that each
has a key that no two of its rows share (a set can give the key of its table under another name: `id AS customer_id`). A retail customer and a corporate one can have the same id, so the id is the key of its set, and a row of the union is
told from another by its set and its id. An association from a class that has sets says which sets it joins, in a section for each: here only the orders of retail
customers are joined (`(source set: retail)`), so an order of customer 1 is the order of Ann, and not of Acme, who has the same id.

## Model: ../models/sql_query_sets_model.md

## DataStore: org_db (Database)

### Schema: org

#### Table: retail_customers

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

#### Table: corporate_customers

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

#### Table: orders → Order

| Column      | Type | Key | Nullable | Property ID |
|-------------|------|-----|----------|-------------|
| id          | INT  | PK  | N        | id          |
| customer_id | INT  |     | Y        |             |
| amount      | INT  |     | Y        | amount      |

#### Query: RETAIL → Customer (set: retail)

```sql
SELECT id AS customer_id, name FROM retail_customers
```

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| customer_id | INT     | PK  | N        | id          |
| name        | VARCHAR |     | Y        | name        |

#### Query: CORPORATE → Customer (set: corporate)

```sql
SELECT id AS customer_id, name FROM corporate_customers
```

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| customer_id | INT     | PK  | N        | id          |
| name        | VARCHAR |     | Y        | name        |

#### Association: CustomerOrders (source set: retail)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| customer_id   | orders       | customer_id   |
