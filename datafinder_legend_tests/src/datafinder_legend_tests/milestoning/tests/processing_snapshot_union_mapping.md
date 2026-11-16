# Legend Processing Snapshot Union Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`processingSnapshotMilestoningUnionMap`).
Order, ProcessingTemporalProduct and ProcessingTemporalProductClassification are each the union of two sets that read the same table, a query for each, so every row is there twice. ProcessingTemporalProduct and
ProcessingTemporalProductClassification are milestoned by the single processing datetime column `snapshotDate`, which the query of each set gives. Every set of a class joins every set of the class it reaches.

## Model: processing_snapshot_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | processing_datetime | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------------|---------------|--------------------|------------------|-------------------|
| processing_snapshot |                  |                | snapshotDate        |               |                    |                  |                   |

### Schema: main

#### Table: OrderTable

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        |             |
| prodFk | INT  |     | Y        |             |

#### Table: ProductTableWithProcessingSnapshotMilestoning

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| id           | INT     | PK  | N        |             |
| name         | VARCHAR |     | Y        |             |
| type         | VARCHAR |     | Y        |             |
| snapshotDate | DATE    |     | Y        |             |

#### Table: ProductClassificationTableWithProcessingSnapshotMilestoning

| Column           | Type    | Key | Nullable | Property ID |
|------------------|---------|-----|----------|-------------|
| type             | VARCHAR | PK  | N        |             |
| type_description | VARCHAR |     | Y        |             |
| snapshotDate     | DATE    |     | Y        |             |

#### Query: ORDER_O → Order (set: o)

```sql
SELECT id, prodFk FROM OrderTable
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        | id          |
| prodFk | INT  |     | Y        |             |

#### Query: ORDER_O2 → Order (set: o2)

```sql
SELECT id, prodFk FROM OrderTable
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        | id          |
| prodFk | INT  |     | Y        |             |

#### Query: PRODUCT_P → ProcessingTemporalProduct (set: p, milestoning: processing_snapshot)

```sql
SELECT id, name, type, snapshotDate FROM ProductTableWithProcessingSnapshotMilestoning
```

| Column       | Type    | Key | Nullable | Property ID  |
|--------------|---------|-----|----------|--------------|
| id           | INT     | PK  | N        | id           |
| name         | VARCHAR |     | Y        | name         |
| type         | VARCHAR |     | Y        |              |
| snapshotDate | DATE    |     | Y        | processingDatetime |

#### Query: PRODUCT_P2 → ProcessingTemporalProduct (set: p2, milestoning: processing_snapshot)

```sql
SELECT id, name, type, snapshotDate FROM ProductTableWithProcessingSnapshotMilestoning
```

| Column       | Type    | Key | Nullable | Property ID  |
|--------------|---------|-----|----------|--------------|
| id           | INT     | PK  | N        | id           |
| name         | VARCHAR |     | Y        | name         |
| type         | VARCHAR |     | Y        |              |
| snapshotDate | DATE    |     | Y        | processingDatetime |

#### Query: CLASSIFICATION_C → ProcessingTemporalProductClassification (set: c, milestoning: processing_snapshot)

```sql
SELECT type, type_description, snapshotDate FROM ProductClassificationTableWithProcessingSnapshotMilestoning
```

| Column           | Type    | Key | Nullable | Property ID  |
|------------------|---------|-----|----------|--------------|
| type             | VARCHAR | PK  | N        | type         |
| type_description | VARCHAR |     | Y        | description  |
| snapshotDate     | DATE    |     | Y        | processingDatetime |

#### Query: CLASSIFICATION_C2 → ProcessingTemporalProductClassification (set: c2, milestoning: processing_snapshot)

```sql
SELECT type, type_description, snapshotDate FROM ProductClassificationTableWithProcessingSnapshotMilestoning
```

| Column           | Type    | Key | Nullable | Property ID  |
|------------------|---------|-----|----------|--------------|
| type             | VARCHAR | PK  | N        | type         |
| type_description | VARCHAR |     | Y        | description  |
| snapshotDate     | DATE    |     | Y        | processingDatetime |

#### Association: OrderProcessingTemporalProduct (source set: o, target set: p)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P | id |

#### Association: OrderProcessingTemporalProduct (source set: o, target set: p2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P2 | id |

#### Association: OrderProcessingTemporalProduct (source set: o2, target set: p)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P | id |

#### Association: OrderProcessingTemporalProduct (source set: o2, target set: p2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P2 | id |

#### Association: ProductClassificationLink (source set: p, target set: c)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| type | CLASSIFICATION_C | type |

#### Association: ProductClassificationLink (source set: p, target set: c2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| type | CLASSIFICATION_C2 | type |

#### Association: ProductClassificationLink (source set: p2, target set: c)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| type | CLASSIFICATION_C | type |

#### Association: ProductClassificationLink (source set: p2, target set: c2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| type | CLASSIFICATION_C2 | type |
