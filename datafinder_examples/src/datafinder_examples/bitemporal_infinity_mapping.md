# Bitemporal Infinity Mapping

## Model: bitemporal_infinity_model.md

## DataStore: quotes_db (Database)

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| bitemporal | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 00:00:00          |                            |

The two axes end a row that has not ended differently. On the processing axis `out_z` holds the infinity
`9999-12-31 00:00:00`, so a query never needs to look for NULL there. On the business axis there is no infinity: `thru_z`
is NULL, and a query has to accept that.

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
