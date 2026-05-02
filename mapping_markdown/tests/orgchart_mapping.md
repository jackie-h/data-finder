# Org Chart Mapping

## Model: orgchart.md

## Repository: orgchart_db

### Schema: hr

#### Table: employees → Employee

| Column     | Type    | Key | Property |
|------------|---------|-----|----------|
| id         | INT     | PK  | id       |
| name       | VARCHAR |     | name     |
| manager_id | INT     | FK  | manager  |

#### Association: EmployeeManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| manager_id    | employees    | id            |
