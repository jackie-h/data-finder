# Bitemporal Dates Mapping

## Model: bitemporal_dates_model.md

## DataStore: dated_quotes_db (Database)

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | processing_infinite_datetime | business_infinite_datetime |
|------------|------------------|----------------|---------------|--------------------|------------------|------------------------------|----------------------------|
| bitemporal | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 00:00:00          | 9999-12-31                 |

A row that has not ended holds an infinity on both axes, a datetime for processing and a date with no time for business,
so a query accepts no NULL end on either.

### Schema: dated

#### Table: quotes → Quote (milestoning: bitemporal)

| Column | Type      | Key | Nullable | Property ID  |
|--------|-----------|-----|----------|--------------|
| code   | VARCHAR   | PK  | N        | code         |
| amount | DOUBLE    |     | Y        | amount       |
| in_z   | TIMESTAMP |     | Y        | validFrom    |
| out_z  | TIMESTAMP |     | Y        | validTo      |
| from_z | DATE      |     | Y        | businessFrom |
| thru_z | DATE      |     | Y        | businessThru |
