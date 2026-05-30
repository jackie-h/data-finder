# Org Chart Model

## Sub-Domain: orgchart

### Class: Person

| Name   | Description    |
|--------|----------------|
| Person | A human being  |

| Property   | Id         | Type    | Key | Description        |
|------------|------------|---------|-----|--------------------|
| Id         | id         | Integer | Y   |                    |
| First Name | firstName  | String  |     |                    |
| Last Name  | lastName   | String  |     |                    |

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

| Property   | Id         | Type   | Key | Description |
|------------|------------|--------|-----|-------------|
| Department | department | String |     |             |

### Association: EmployeeManager

| Name            | Source   | Source Property | Source Multiplicity | Target   | Target Property | Target Multiplicity | Description                        |
|-----------------|----------|-----------------|---------------------|----------|-----------------|---------------------|------------------------------------|
| EmployeeManager | Employee | employees       | *                   | Employee | manager         | 1                   | Links an employee to their manager |

### Class: Project

| Name    | Description                        |
|---------|------------------------------------|
| Project | A project that employees work on   |

| Property | Id   | Type    | Key | Description     |
|----------|------|---------|-----|-----------------|
| Id       | id   | Integer | Y   |                 |
| Name     | name | String  |     | Project name    |
| Code     | code | String  |     | Short code      |

### Association: EmployeeProject

| Name            | Source  | Source Property | Source Multiplicity | Target   | Target Property | Target Multiplicity | Description                               |
|-----------------|---------|-----------------|---------------------|----------|-----------------|---------------------|-------------------------------------------|
| EmployeeProject | Project | projects        | *                   | Employee | assignee        | 1                   | Links a project to its assigned employee  |
