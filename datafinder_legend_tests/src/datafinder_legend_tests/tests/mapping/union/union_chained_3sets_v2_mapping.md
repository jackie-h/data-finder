# Legend Union With Chained Joins Mapping: 3 sets, V2

Source: legend-engine core_relational `tests/mapping/union/testUnionWithMultipleChainedJoins.pure` (`multipleChainedJoinsMappingWithUnionAcross3SetsV2`).
In this variant X and Y are unions, set i of X joins set i of Y, and every set of Y joins Z, which is its set 0 alone. A union of 3 milestoned tables is a set for each, a query of its own with the business date range of its rows; a class that is not a union is
its first table. Every table has the same fk, so which sets join is said by the pairs of sets of its associations: a pair that is not named does not join (Legend says the
same with a join for each pair of sets, through link tables that hold the same fk, and so keep every row).

## Model: union_chained_model.md

## DataStore: multipleChainedJoinsDB (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

### Schema: main

#### Table: X0

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: X1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: X2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: Y0

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: Y1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: Y2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: Z1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Table: Z2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |
| from_z | DATE |     | Y        |             |
| thru_z | DATE |     | Y        |             |

#### Query: X_SET0 → X (set: x0, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM X0
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Query: X_SET1 → X (set: x1, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM X1
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Query: X_SET2 → X (set: x2, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM X2
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Association: XY (source set: x0, target set: y0)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y_SET0 | fk |

#### Association: XY (source set: x1, target set: y1)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y_SET1 | fk |

#### Association: XY (source set: x2, target set: y2)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y_SET2 | fk |

#### Query: Y_SET0 → Y (set: y0, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM Y0
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Query: Y_SET1 → Y (set: y1, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM Y1
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Query: Y_SET2 → Y (set: y2, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM Y2
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Association: YZ (source set: y0)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Z0 | fk |

#### Association: YZ (source set: y1)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Z0 | fk |

#### Association: YZ (source set: y2)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Z0 | fk |

#### Table: Z0 → Z (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |
