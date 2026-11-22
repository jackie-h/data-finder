# Milestoned Union Mapping

Orders, products and product classifications, where each class is the union of two sets that read the same table, a query for each, so every row is there twice. Product and
ProductClassification are milestoned by the single business date column `snapshotDate`, which the query of each set gives, and Order is not milestoned. Every set of a class
joins every set of the class it reaches, so a row that is read through two steps is found once for each pair of sets.

## Model: ../models/milestoned_union_model.md

## DataStore: db (Database)

| Scheme            | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|-------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_snapshot |                  |                | snapshotDate  |                    |                  |                   |

### Schema: main

#### Table: OrderTable

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        |             |
| prodFk | INT  |     | Y        |             |

#### Table: ProductTableWithBusinessSnapshotMilestoning

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| id           | INT     | PK  | N        |             |
| name         | VARCHAR |     | Y        |             |
| type         | VARCHAR |     | Y        |             |
| snapshotDate | DATE    |     | Y        |             |

#### Table: ProductClassificationTableWithBusinessSnapshotMilestoning

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

#### Query: PRODUCT_P → Product (set: p, milestoning: business_snapshot)

```sql
SELECT id, name, type, snapshotDate FROM ProductTableWithBusinessSnapshotMilestoning
```

| Column       | Type    | Key | Nullable | Property ID  |
|--------------|---------|-----|----------|--------------|
| id           | INT     | PK  | N        | id           |
| name         | VARCHAR |     | Y        | name         |
| type         | VARCHAR |     | Y        |              |
| snapshotDate | DATE    |     | Y        | businessDate |

#### Query: PRODUCT_P2 → Product (set: p2, milestoning: business_snapshot)

```sql
SELECT id, name, type, snapshotDate FROM ProductTableWithBusinessSnapshotMilestoning
```

| Column       | Type    | Key | Nullable | Property ID  |
|--------------|---------|-----|----------|--------------|
| id           | INT     | PK  | N        | id           |
| name         | VARCHAR |     | Y        | name         |
| type         | VARCHAR |     | Y        |              |
| snapshotDate | DATE    |     | Y        | businessDate |

#### Query: CLASSIFICATION_C → ProductClassification (set: c, milestoning: business_snapshot)

```sql
SELECT type, type_description, snapshotDate FROM ProductClassificationTableWithBusinessSnapshotMilestoning
```

| Column           | Type    | Key | Nullable | Property ID  |
|------------------|---------|-----|----------|--------------|
| type             | VARCHAR | PK  | N        | type         |
| type_description | VARCHAR |     | Y        | description  |
| snapshotDate     | DATE    |     | Y        | businessDate |

#### Query: CLASSIFICATION_C2 → ProductClassification (set: c2, milestoning: business_snapshot)

```sql
SELECT type, type_description, snapshotDate FROM ProductClassificationTableWithBusinessSnapshotMilestoning
```

| Column           | Type    | Key | Nullable | Property ID  |
|------------------|---------|-----|----------|--------------|
| type             | VARCHAR | PK  | N        | type         |
| type_description | VARCHAR |     | Y        | description  |
| snapshotDate     | DATE    |     | Y        | businessDate |

#### Association: OrderProduct (source set: o, target set: p)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P | id |

#### Association: OrderProduct (source set: o, target set: p2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P2 | id |

#### Association: OrderProduct (source set: o2, target set: p)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk | PRODUCT_P | id |

#### Association: OrderProduct (source set: o2, target set: p2)

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
