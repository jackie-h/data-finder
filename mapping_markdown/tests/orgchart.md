# Org Chart Model

## Sub-Domain: orgchart

### Class: Employee

| Name     | Description                  |
|----------|------------------------------|
| Employee | An employee in the org chart |

| Property | Id      | Type     | Key | Description             |
|----------|---------|----------|-----|-------------------------|
| Id       | id      | Integer  | Y   |                         |
| Name     | name    | String   |     | Full name of employee   |
| Manager  | manager | Employee |     | Direct reporting manager |

### Association: EmployeeManager

| Name            | Source   | Source Multiplicity | Target   | Target Multiplicity | Description                        |
|-----------------|----------|---------------------|----------|---------------------|------------------------------------|
| EmployeeManager | Employee | *                   | Employee | 1                   | Links an employee to their manager |
