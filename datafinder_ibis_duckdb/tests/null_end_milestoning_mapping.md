# Null End Milestoning Mapping

## Model: null_end_milestoning_model.md

## DataStore: prices_db (Database)

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|-----------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| processing_only | in_z             | out_z          |               |                    |                  |                   |

### Schema: mkt

#### Table: prices → Price (milestoning: processing_only)

| Column | Type      | Key | Property   |
|--------|-----------|-----|------------|
| sym    | VARCHAR   | PK  | symbol     |
| price  | DOUBLE    |     | price      |
| in_z   | TIMESTAMP |     | valid_from |
| out_z  | TIMESTAMP |     | valid_to   |
