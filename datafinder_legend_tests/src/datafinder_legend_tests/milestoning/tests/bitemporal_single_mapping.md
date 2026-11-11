# Legend Single Temporal Store Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure`
(`singleTemporalMappingForBiTemporalTypes`: BiTemporalProduct and BiTemporalProductClassification). The two bitemporal
types are mapped to ProductTable and ProductClassificationTable, which have a business range (`from_z` up to but not
including `thru_z`, the infinity being 9999-12-31) and no processing columns, so a processing date a query gives them is
ignored.

## Model: bitemporal_single_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

### Schema: main

#### Table: ProductTable → BiTemporalProduct (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | biTemporalClassification |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Association: BiTemporalProductClassificationLink

| Source Column | Target Table               | Target Column |
|---------------|----------------------------|---------------|
| type          | ProductClassificationTable | type          |

#### Table: ProductClassificationTable → BiTemporalProductClassification (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |
