# Business Date Range Infinity Mapping

## Model: business_range_model.md

## DataStore: ranged_rates_db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | business_infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|----------------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31                 |

A row that has not ended holds the date 9999-12-31 in `thru_z`, so a query accepts no NULL end. The date is the scheme's
`business_infinite_datetime` alone: a business range has no processing axis to give one to.

### Schema: ranged

#### Table: rates → Rate (milestoning: business_date_range)

| Column | Type    | Key | Nullable | Property ID  |
|--------|---------|-----|----------|--------------|
| code   | VARCHAR | PK  | N        | code         |
| amount | DOUBLE  |     | Y        | amount       |
| from_z | DATE    |     | Y        | businessFrom |
| thru_z | DATE    |     | Y        | businessThru |
