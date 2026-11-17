# Table Milestoning Mapping

## Model: table_milestoning_model.md

## DataStore: markets_db (Database)

| Scheme   | processing_start | processing_end | processing_infinite_datetime |
|----------|------------------|----------------|------------------------------|
| standard | in_z             | out_z          | 9999-12-31 00:00:00          |

The processing dimension is the same for every table, so `standard` has only that. A table names it and gives the business dimension it has
itself, by mapping its columns to `businessFrom` and `businessThru`, or to `businessDate`. A table that maps a column to `validFrom` or any of
the others has that column where the scheme has its own.

### Schema: mkt

#### Table: quotes → Quote (milestoning: standard, business_infinity: 9999-12-31)

| Column | Type      | Key | Nullable | Property ID  |
|--------|-----------|-----|----------|--------------|
| code   | VARCHAR   | PK  | N        | code         |
| amount | DOUBLE    |     | Y        | amount       |
| in_z   | TIMESTAMP |     | Y        | validFrom    |
| out_z  | TIMESTAMP |     | Y        | validTo      |
| from_z | DATE      |     | Y        | businessFrom |
| thru_z | DATE      |     | Y        | businessThru |

#### Table: rates → Rate (milestoning: standard)

| Column    | Type      | Key | Nullable | Property ID  |
|-----------|-----------|-----|----------|--------------|
| code      | VARCHAR   | PK  | N        | code         |
| rate      | DOUBLE    |     | Y        | rate         |
| in_z      | TIMESTAMP |     | Y        | validFrom    |
| out_z     | TIMESTAMP |     | Y        | validTo      |
| rate_date | DATE      |     | Y        | businessDate |

#### Table: fees → Fee (milestoning: standard)

| Column      | Type      | Key | Nullable | Property ID |
|-------------|-----------|-----|----------|-------------|
| code        | VARCHAR   | PK  | N        | code        |
| amount      | DOUBLE    |     | Y        | amount      |
| recorded_in | TIMESTAMP |     | Y        | validFrom   |
| recorded_to | TIMESTAMP |     | Y        | validTo     |
