# Org Chart Model

## Sub-Domain: orgchart

### Class: Person

| Name   | Description    |
|--------|----------------|
| Person | A human being  |

| Property   | Id         | Type    | Key | Description        |
|------------|------------|---------|-----|--------------------|
| Id         | id         | Integer | Y   |                    |
| First Name | first_name | String  |     |                    |
| Last Name  | last_name  | String  |     |                    |

### Class: Contactable

| Name        | Description                  |
|-------------|------------------------------|
| Contactable | Something that can be emailed |

| Property | Id    | Type   | Key | Description   |
|----------|-------|--------|-----|---------------|
| Email    | email | String |     | Email address |

### Class: Employee extends Person, Contactable

| Name     | Description                  |
|----------|------------------------------|
| Employee | An employee in the org chart |

| Property   | Id         | Type     | Key | Description              |
|------------|------------|----------|-----|--------------------------|
| Department | department | String   |     |                          |
| Manager    | manager    | Employee |     | Direct reporting manager |

### Association: EmployeeManager

| Name            | Source   | Target   | Description                        |
|-----------------|----------|----------|------------------------------------|
| EmployeeManager | Employee | Employee | Links an employee to their manager |
