# Legend Milestoning Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`milestoningmap`, bitemporal tables only).
The Legend tables carry in_z/out_z (processing) and from_z/thru_z (business) range columns.

## Model: milestoning_model.md

## DataStore: db (Database)

| Scheme     | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime   |
|------------|------------------|----------------|---------------|--------------------|------------------|---------------------|
| bitemporal | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 23:59:59 |

### Schema: main

#### Table: BiTemporalProductTable → BiTemporalProduct (milestoning: bitemporal)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| id     | INT       | PK  | id          |
| type   | VARCHAR   | FK  | biTemporalClassification |
| in_z   | TIMESTAMP |     | validFrom   |
| out_z  | TIMESTAMP |     | validTo     |
| from_z | TIMESTAMP |     | businessFrom |
| thru_z | TIMESTAMP |     | businessThru |

#### Association: BiTemporalProductClassificationLink

| Source Column | Target Table                       | Target Column |
|---------------|------------------------------------|---------------|
| type          | BiTemporalProductClassificationTable | type        |

#### Table: BiTemporalProductClassificationTable → BiTemporalProductClassification (milestoning: bitemporal)

| Column | Type      | Key | Property ID |
|--------|-----------|-----|-------------|
| type   | VARCHAR   | PK  | type        |
| system | VARCHAR   |     | system      |
| exchange | VARCHAR |     | exchange    |
| in_z   | TIMESTAMP |     | validFrom   |
| out_z  | TIMESTAMP |     | validTo     |
| from_z | TIMESTAMP |     | businessFrom |
| thru_z | TIMESTAMP |     | businessThru |
