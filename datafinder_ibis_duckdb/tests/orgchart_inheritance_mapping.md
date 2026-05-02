# Org Chart Inheritance Mapping

## Model: ../../model_markdown/tests/orgchart_inheritance.md

## Repository: hr_db

### Schema: hr

#### Table: employees → Employee

| Column     | Type    | Key | Property   |
|------------|---------|-----|------------|
| emp_id     | INT     | PK  | id         |
| first_name | VARCHAR |     | first_name |
| last_name  | VARCHAR |     | last_name  |
| email      | VARCHAR |     | email      |
| department | VARCHAR |     | department |
| manager_id | INT     | FK  | manager    |

#### Association: EmployeeManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| manager_id    | employees    | emp_id        |
