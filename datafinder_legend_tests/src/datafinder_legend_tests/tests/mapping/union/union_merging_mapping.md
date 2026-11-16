# Legend Union For SQL Query Merging Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnionForSQLQueryMerging.pure` (`unionMapping`, without the properties that read through inner joins).
A, B, C and D are each the union of two sets, a query for each of the tables aTable1 and aTable2, and so on. Each set joins the set of the next class that has the same number: the first sets
join each other, and the second sets join each other, on `fk`, and a first set does not join a second set.

## Model: union_merging_model.md

## DataStore: database (Database)

### Schema: main

#### Table: aTable1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: aTable2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: bTable1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: bTable2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: cTable1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: cTable2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: dTable1

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Table: dTable2

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        |             |
| fk     | INT  |     | Y        |             |

#### Query: A_SET_1 → A (set: a1)

```sql
SELECT pk, fk FROM aTable1
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: A_SET_2 → A (set: a2)

```sql
SELECT pk, fk FROM aTable2
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: B_SET_1 → B (set: b1)

```sql
SELECT pk, fk FROM bTable1
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: B_SET_2 → B (set: b2)

```sql
SELECT pk, fk FROM bTable2
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: C_SET_1 → C (set: c1)

```sql
SELECT pk, fk FROM cTable1
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: C_SET_2 → C (set: c2)

```sql
SELECT pk, fk FROM cTable2
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: D_SET_1 → D (set: d1)

```sql
SELECT pk, fk FROM dTable1
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Query: D_SET_2 → D (set: d2)

```sql
SELECT pk, fk FROM dTable2
```

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| pk     | INT  | PK  | N        | pk          |
| fk     | INT  |     | Y        |             |

#### Association: AB (source set: a1, target set: b1)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | B_SET_1  | fk            |

#### Association: AB (source set: a2, target set: b2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | B_SET_2  | fk            |

#### Association: BC (source set: b1, target set: c1)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | C_SET_1  | fk            |

#### Association: BC (source set: b2, target set: c2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | C_SET_2  | fk            |

#### Association: CD (source set: c1, target set: d1)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | D_SET_1  | fk            |

#### Association: CD (source set: c2, target set: d2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| fk            | D_SET_2  | fk            |
