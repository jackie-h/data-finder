# Legend Business Snapshot Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure`
(`businessSnapshotMilestoningMap`). Product and ProductClassification have a single business date column,
`snapshotDate`, which a query's business date must equal. Order has no milestoning.

## Model: business_snapshot_model.md

## DataStore: db (Database)

| Scheme            | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|-------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_snapshot |                  |                | snapshotDate  |                    |                  |                   |

### Schema: main

#### Table: OrderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| prodFk | INT | FK | Y | product |

#### Association: OrderProduct

| Source Column | Target Table                                | Target Column |
|---------------|---------------------------------------------|---------------|
| prodFk        | ProductTableWithBusinessSnapshotMilestoning | id            |

#### Table: ProductTableWithBusinessSnapshotMilestoning → Product (milestoning: business_snapshot)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | classification |
| snapshotDate | DATE |  | Y | businessDate |

#### Association: ProductClassificationLink

| Source Column | Target Table                                            | Target Column |
|---------------|---------------------------------------------------------|---------------|
| type          | ProductClassificationTableWithBusinessSnapshotMilestoning | type          |

#### Table: ProductClassificationTableWithBusinessSnapshotMilestoning → ProductClassification (milestoning: business_snapshot)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| type_description | VARCHAR |  | Y | description |
| snapshotDate | DATE |  | Y | businessDate |
