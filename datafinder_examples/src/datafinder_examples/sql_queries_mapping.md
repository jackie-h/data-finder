# SQL Queries Mapping

A class can be mapped to a query as it is mapped to a table. The query is a `Query` section: its heading names the query and the class, the
SQL follows in a `sql` block, written as PostgreSQL, and then the columns it gives and what they map to, as a Table section has them. The
SQL is checked when the mapping is loaded: the tables and columns it reads must exist, and the columns it gives must be the ones the section
lists. Nothing is created in the database: a query over the class is a query over the SQL, which is written into it as a derived table.

## Model: sql_queries_model.md

## DataStore: org_db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| business_date_range |                  |                |               | from_z             | thru_z           |                              |                            |
| bitemporal          | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 00:00:00          | 9999-12-31                 |

### Schema: org

#### Table: people → Person

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| person_id  | INT     | PK  | N        | id          |
| first_name | VARCHAR |     | Y        | firstName   |
| age        | INT     |     | Y        | age         |
| firm_id    | INT     |     | Y        |             |
| firm_name  | VARCHAR |     | Y        |             |

#### Table: firms

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

#### Table: trades

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| acc_num | INT     |     | Y        |             |
| gsn     | VARCHAR |     | Y        |             |
| qty     | INT     |     | Y        |             |

#### Query: FIRM_VIEW → Firm

```sql
SELECT DISTINCT firm_id, firm_name
FROM people
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| firm_id   | INT     | PK  | N        | id          |
| firm_name | VARCHAR |     | Y        | legalName   |

#### Association: FirmPerson

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| firm_id       | people       | firm_id       |

#### Query: POSITION_VIEW → Position

```sql
SELECT gsn, SUM(qty) AS quantity
FROM trades
GROUP BY gsn
```

| Column   | Type    | Key | Nullable | Property ID |
|----------|---------|-----|----------|-------------|
| gsn      | VARCHAR | PK  | N        | gsn         |
| quantity | INT     |     | Y        | quantity    |

#### Query: FIRM_X_PEOPLE → FirmXPerson

```sql
SELECT p.person_id, p.first_name
FROM people p
INNER JOIN firms f ON f.id = p.firm_id
WHERE f.name = 'Firm X'
```

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| person_id  | INT     | PK  | N        | id          |
| first_name | VARCHAR |     | Y        | firstName   |

#### Query: TAG_VIEW → Tag

```sql
SELECT person_id, 'Mr ' || first_name AS text
FROM people
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| person_id | INT     | PK  | N        | id          |
| text      | VARCHAR |     | Y        | text        |

A query can read another query, which is read as a derived table inside it.

#### Query: SHOUT_VIEW → Shout

```sql
SELECT firm_id, UPPER(firm_name) AS text
FROM FIRM_VIEW
```

| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| firm_id | INT     | PK  | N        | id          |
| text    | VARCHAR |     | Y        | text        |

#### Table: rates

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| code   | VARCHAR |     | Y        |             |
| amount | DOUBLE  |     | Y        |             |
| from_z | DATE    |     | Y        |             |
| thru_z | DATE    |     | Y        |             |

#### Table: quotes

| Column | Type      | Key | Nullable | Property ID |
|--------|-----------|-----|----------|-------------|
| code   | VARCHAR   |     | Y        |             |
| amount | DOUBLE    |     | Y        |             |
| in_z   | TIMESTAMP |     | Y        |             |
| out_z  | TIMESTAMP |     | Y        |             |
| from_z | DATE      |     | Y        |             |
| thru_z | DATE      |     | Y        |             |

#### Table: holders → Holder

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| id        | INT     | PK  | N        | id          |
| rate_code | VARCHAR | FK  | Y        |             |

A query is milestoned as a table is, by the columns its SQL gives: the scheme is named in the heading, and the columns the scheme names carry the
properties of its columns. A query over the class is then given the dates, and a join to it is given them too.

#### Query: RATE_QUERY → Rate (milestoning: business_date_range)

```sql
SELECT code, amount, from_z, thru_z
FROM rates
WHERE amount > 0
```

| Column | Type    | Key | Nullable | Property ID  |
|--------|---------|-----|----------|--------------|
| code   | VARCHAR | PK  | N        | code         |
| amount | DOUBLE  |     | Y        | amount       |
| from_z | DATE    |     | Y        | businessFrom |
| thru_z | DATE    |     | Y        | businessThru |

#### Association: HolderRate

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| rate_code     | RATE_QUERY   | code          |

#### Query: QUOTE_QUERY → Quote (milestoning: bitemporal)

```sql
SELECT code, amount, in_z, out_z, from_z, thru_z
FROM quotes
WHERE amount IS NOT NULL
```

| Column | Type      | Key | Nullable | Property ID  |
|--------|-----------|-----|----------|--------------|
| code   | VARCHAR   | PK  | N        | code         |
| amount | DOUBLE    |     | Y        | amount       |
| in_z   | TIMESTAMP |     | Y        | validFrom    |
| out_z  | TIMESTAMP |     | Y        | validTo      |
| from_z | DATE      |     | Y        | businessFrom |
| thru_z | DATE      |     | Y        | businessThru |
