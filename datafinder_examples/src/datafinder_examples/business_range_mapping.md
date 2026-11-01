# Business Date Range Mapping

## Model: business_range_model.md

## DataStore: rates_db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to |
|---------------------|------------------|----------------|---------------|--------------------|------------------|
| business_date_range |                  |                |               | from_z             | thru_z           |

### Schema: mkt

#### Table: rates → Rate (milestoning: business_date_range)

| Column | Type    | Key | Nullable | Property ID  |
|--------|---------|-----|----------|--------------|
| code   | VARCHAR | PK  | N        | code         |
| amount | DOUBLE  |     | Y        | amount       |
| from_z | DATE    |     | Y        | businessFrom |
| thru_z | DATE    |     | Y        | businessThru |
