# Org Chart Mapping

## Model: orgchart.md

## DataStore: orgchart_db (Database)

### Schema: hr

#### Table: employees → Employee

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|----------|
| id         | INT     | PK  | N        | id       |
| name       | VARCHAR |     | Y        | name     |
| manager_id | INT     | FK  | Y        | manager  |

#### Association: EmployeeManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| manager_id    | employees    | id            |
