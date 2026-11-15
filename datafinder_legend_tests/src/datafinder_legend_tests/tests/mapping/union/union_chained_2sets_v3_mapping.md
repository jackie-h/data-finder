# Legend Union With Chained Joins Mapping: 2 sets, V3

Source: legend-engine core_relational `tests/mapping/union/testUnionWithMultipleChainedJoins.pure` (`multipleChainedJoinsMappingWithUnionAcross2SetsV3`).
In this variant X is a union, every set of X joins Y, which is its set 0 alone, and that joins Z, also its set 0 alone. A union of 2 milestoned tables is a set for each, a query of its own with the business date range of its rows; a class that is not a union is
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

#### Table: Y1

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

#### Association: XY (source set: x0)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y0 | fk |

#### Association: XY (source set: x1)

| Source Column | Target Table | Target Column |
|---|---|---|
| fk | Y0 | fk |

#### Table: Y0 → Y (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| pk     | INT  | PK  | N        | pk           |
| fk     | INT  |     | Y        |              |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Association: YZ

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
