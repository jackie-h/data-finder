# Legend Union With Chained Joins Mapping: 2 sets, V1

Source: legend-engine core_relational `tests/mapping/union/testUnionWithMultipleChainedJoins.pure` (`multipleChainedJoinsMappingWithUnionAcross2SetsV1`).
In this variant X, Y and Z are each a union, and set i of X joins set i of Y, and set i of Y set i of Z. A union of 2 milestoned tables is a set for each, a query of its own with the business date range of its rows; a class that is not a union is
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

#### Table: Z0

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

#### Association: XY (source set: x0, target set: y0)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y_SET0 | fk |

#### Association: XY (source set: x1, target set: y1)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y_SET1 | fk |

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

#### Association: YZ (source set: y0, target set: z0)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Z_SET0 | fk |

#### Association: YZ (source set: y1, target set: z1)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Z_SET1 | fk |

#### Query: Z_SET0 → Z (set: z0, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM Z0
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Query: Z_SET1 → Z (set: z1, milestoning: business_date_range)

```sql
SELECT pk, fk, from_z, thru_z FROM Z1
```

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |
