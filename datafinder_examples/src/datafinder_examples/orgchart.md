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

### Association: EmployeeManager

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description                        |
|-----------------|----------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|------------------------------------|
| EmployeeManager | Employee | Employees            | employees          | *                   | Employee | Manager              | manager            | 1                   | Links an employee to their manager |
