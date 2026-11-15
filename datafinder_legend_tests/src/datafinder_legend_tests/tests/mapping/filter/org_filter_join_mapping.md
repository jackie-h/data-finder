# Legend Filter Mapping Tree Mapping

Source: legend-engine core_relational `tests/mapping/filter/testFilterMappingTree.pure` (`orgTestMappingWithJoin`: Org is the rows of orgTable that have a row
in otherTable with `filterVal <= 4`, as `~filter` through a join). It is a `Query` section here, and the parent of an Org is a row of that query.

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
SELECT o.id, o.name, o.parentId
FROM orgTable o
INNER JOIN otherTable t ON o.id = t.orgTableId
WHERE t.filterVal <= 4
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
