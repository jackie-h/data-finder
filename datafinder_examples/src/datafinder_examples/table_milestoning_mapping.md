# Table Milestoning Mapping

## Model: table_milestoning_model.md

## DataStore: markets_db (Database)

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| bitemporal | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 00:00:00          |                            |

`bitemporal` is how the tables are milestoned unless one says otherwise. A table that names it and maps a column to `validFrom`, `validTo`,
`businessFrom`, `businessThru` or `businessDate` has that column in place of the scheme's, and the rest of the scheme is its own. A
business date in place of a business range replaces the range.

### Schema: mkt

#### Table: quotes → Quote (milestoning: bitemporal)

| Column | Type      | Key | Nullable | Property ID  |
|--------|-----------|-----|----------|--------------|
| code   | VARCHAR   | PK  | N        | code         |
| amount | DOUBLE    |     | Y        | amount       |
| in_z   | TIMESTAMP |     | Y        | validFrom    |
| out_z  | TIMESTAMP |     | Y        | validTo      |
| from_z | DATE      |     | Y        | businessFrom |
| thru_z | DATE      |     | Y        | businessThru |

#### Table: rates → Rate (milestoning: bitemporal, business_infinity: 9999-12-31)

| Column     | Type      | Key | Nullable | Property ID  |
|------------|-----------|-----|----------|--------------|
| code       | VARCHAR   | PK  | N        | code         |
| rate       | DOUBLE    |     | Y        | rate         |
| in_z       | TIMESTAMP |     | Y        | validFrom    |
| out_z      | TIMESTAMP |     | Y        | validTo      |
| valid_from | DATE      |     | Y        | businessFrom |
| valid_to   | DATE      |     | Y        | businessThru |

#### Table: fees → Fee (milestoning: bitemporal)

| Column   | Type      | Key | Nullable | Property ID  |
|----------|-----------|-----|----------|--------------|
| code     | VARCHAR   | PK  | N        | code         |
| amount   | DOUBLE    |     | Y        | amount       |
| in_z     | TIMESTAMP |     | Y        | validFrom    |
| out_z    | TIMESTAMP |     | Y        | validTo      |
| fee_date | DATE      |     | Y        | businessDate |
