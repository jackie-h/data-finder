# Org Chart Model

## Sub-Domain: orgchart

### Class: Person

| Name   | Description    |
|--------|----------------|
| Person | A human being  |

| Property   | Id         | Type    | Multiplicity | Key | Description        |
|------------|------------|---------|--------------|-----|--------------------|
| Id         | id         | Integer | 1            | Y   |                    |
| First Name | firstName  | String  | 1            |     |                    |
| Last Name  | lastName   | String  | 1            |     |                    |

### Class: Contactable

| Name        | Description                  |
|-------------|------------------------------|
| Contactable | Something that can be emailed |

| Property | Id    | Type   | Multiplicity | Key | Description   |
|----------|-------|--------|--------------|-----|---------------|
| Email    | email | String | 0..1         |     | Email address |

### Class: Employee extends Person, Contactable

| Name     | Description                  |
|----------|------------------------------|
| Employee | An employee in the org chart |

| Property   | Id         | Type   | Multiplicity | Key | Description |
|------------|------------|--------|--------------|-----|-------------|
| Department | department | String | 0..1         |     |             |

### Association: EmployeeManager

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description                        |
|-----------------|----------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|------------------------------------|
| EmployeeManager | Employee | Employees            | employees          | *                   | Employee | Manager              | manager            | 0..1                | Links an employee to their manager |

### Class: Project

| Name    | Description                        |
|---------|------------------------------------|
| Project | A project that employees work on   |

| Property | Id   | Type    | Multiplicity | Key | Description     |
|----------|------|---------|--------------|-----|-----------------|
| Id       | id   | Integer | 1            | Y   |                 |
| Name     | name | String  | 1            |     | Project name    |
| Code     | code | String  | 1            |     | Short code      |

### Association: EmployeeProject

| Name            | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description                               |
|-----------------|---------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------------------------------------|
| EmployeeProject | Project | Projects             | projects           | *                   | Employee | Assignee             | assignee           | *                   | Links a project to its assigned employee  |
