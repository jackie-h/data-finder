# Legend Filter Mapping Tree Mapping

Source: legend-engine core_relational `tests/mapping/filter/testFilterMappingTree.pure` (`orgTestMapping`: Org is the rows of orgTable with
`filterVal <= 4`, as `~filter`). It is a `Query` section here, and the parent of an Org is a row of that query.

## Model: org_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: orgTable

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| id        | INT     | PK  | N        |             |
| filterVal | INT     |     | Y        |             |
| parentId  | INT     |     | Y        |             |
| name      | VARCHAR |     | Y        |             |

#### Table: otherTable

| Column     | Type | Key | Nullable | Property ID |
|------------|------|-----|----------|-------------|
| orgTableId | INT  | PK  | N        |             |
| filterVal  | INT  |     | Y        |             |

#### Query: ORG → Org

```sql
SELECT id, name, parentId
FROM orgTable
WHERE filterVal <= 4
```

| Column   | Type    | Key | Nullable | Property ID |
|----------|---------|-----|----------|-------------|
| id       | INT     | PK  | N        | id          |
| name     | VARCHAR |     | Y        | name        |
| parentId | INT     |     | Y        |             |

#### Association: OrgOrg

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| parentId      | ORG          | id            |
