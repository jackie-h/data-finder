# Legend Processing Snapshot Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure`
(`processingSnapshotMilestoningMap`). Product and ProductClassification have a single processing datetime column,
`snapshotDate`, which a query's processing time must equal. Order has no milestoning.

## Model: processing_snapshot_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | processing_datetime | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------------|---------------|--------------------|------------------|-------------------|
| processing_snapshot |                  |                | snapshotDate        |               |                    |                  |                   |

### Schema: main

#### Table: OrderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| prodFk | INT | FK | Y | processingTemporalProduct |

#### Association: OrderProcessingTemporalProduct

| Source Column | Target Table                                | Target Column |
|---------------|---------------------------------------------|---------------|
| prodFk        | ProductTableWithProcessingSnapshotMilestoning | id            |

#### Table: ProductTableWithProcessingSnapshotMilestoning → ProcessingTemporalProduct (milestoning: processing_snapshot)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | classification |
| snapshotDate | DATE |  | Y | processingDatetime |

#### Association: ProductClassificationLink

| Source Column | Target Table                                            | Target Column |
|---------------|---------------------------------------------------------|---------------|
| type          | ProductClassificationTableWithProcessingSnapshotMilestoning | type          |

#### Table: ProductClassificationTableWithProcessingSnapshotMilestoning → ProcessingTemporalProductClassification (milestoning: processing_snapshot)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| type_description | VARCHAR |  | Y | description |
| snapshotDate | DATE |  | Y | processingDatetime |
