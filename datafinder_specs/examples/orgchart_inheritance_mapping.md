# Org Chart Inheritance Mapping

## Model: orgchart_inheritance.md

## DataStore: hr_db (Database)

### Schema: hr

#### Table: employees → Employee

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|------------|
| emp_id     | INT     | PK  | N        | id         |
| first_name | VARCHAR |     | Y        | firstName  |
| last_name  | VARCHAR |     | Y        | lastName   |
| email      | VARCHAR |     | Y        | email      |
| department | VARCHAR |     | Y        | department |
| manager_id | INT     | FK  | Y        | manager    |

#### Association: EmployeeManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| manager_id    | employees    | emp_id        |

#### Table: projects → Project

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|----------|
| project_id  | INT     | PK  | N        | id       |
| name        | VARCHAR |     | Y        | name     |
| code        | VARCHAR |     | Y        | code     |
| assignee_id | INT     | FK  | Y        | assignee |

#### Association: EmployeeProject

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| assignee_id   | employees    | emp_id        |
