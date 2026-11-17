# Schema Milestoning Mapping

## Model: schema_milestoning_model.md

## DataStore: markets_db (Database)

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| bitemporal | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 00:00:00          |                            |

The data store's `bitemporal` is the one every schema uses unless it defines a scheme of the same name itself.

### Schema: mkt

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| bitemporal | recorded_from    | recorded_to    |               | valid_from         | valid_to         |                              | 9999-12-31                 |

The `mkt` schema keeps its columns under other names, and ends its business range in an infinity and its processing range in NULL.

#### Table: quotes → Quote (milestoning: bitemporal)

| Column        | Type      | Key | Nullable | Property ID  |
|---------------|-----------|-----|----------|--------------|
| code          | VARCHAR   | PK  | N        | code         |
| amount        | DOUBLE    |     | Y        | amount       |
| recorded_from | TIMESTAMP |     | Y        | validFrom    |
| recorded_to   | TIMESTAMP |     | Y        | validTo      |
| valid_from    | DATE      |     | Y        | businessFrom |
| valid_to      | DATE      |     | Y        | businessThru |

### Schema: ref

#### Table: rates → Rate (milestoning: bitemporal)

| Column | Type      | Key | Nullable | Property ID  |
|--------|-----------|-----|----------|--------------|
| code   | VARCHAR   | PK  | N        | code         |
| rate   | DOUBLE    |     | Y        | rate         |
| in_z   | TIMESTAMP |     | Y        | validFrom    |
| out_z  | TIMESTAMP |     | Y        | validTo      |
| from_z | DATE      |     | Y        | businessFrom |
| thru_z | DATE      |     | Y        | businessThru |
