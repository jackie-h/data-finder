# Org Chart Inheritance Mapping

## Model: orgchart_inheritance.md

## DataStore: hr_db (Database)

### Schema: hr

#### Table: employees → Employee

| Column     | Type    | Key | Property ID |
|------------|---------|-----|------------|
| emp_id     | INT     | PK  | id         |
| first_name | VARCHAR |     | firstName  |
| last_name  | VARCHAR |     | lastName   |
| email      | VARCHAR |     | email      |
| department | VARCHAR |     | department |
| manager_id | INT     | FK  | manager    |

#### Association: EmployeeManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| manager_id    | employees    | emp_id        |

#### Table: projects → Project

| Column      | Type    | Key | Property ID |
|-------------|---------|-----|----------|
| project_id  | INT     | PK  | id       |
| name        | VARCHAR |     | name     |
| code        | VARCHAR |     | code     |
| assignee_id | INT     | FK  | assignee |

#### Association: EmployeeProject

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| assignee_id   | employees    | emp_id        |
