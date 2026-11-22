# Org Chart Inheritance GraphQL Mapping

## Model: orgchart_inheritance.md

## Endpoint: http://localhost:4000/graphql (filter: where, sort: order_by, limit: limit)

### Query: employees → Employee

| Field      | Property ID |
|------------|-------------|
| id         | id          |
| firstName  | firstName   |
| lastName   | lastName    |
| email      | email       |
| department | department  |

#### Association: EmployeeManager

| GraphQL Field |
|---------------|
| manager       |

#### Association: EmployeeProject

| GraphQL Field |
|---------------|
| projects      |

### Query: projects → Project

| Field | Property ID |
|-------|-------------|
| id    | id          |
| name  | name        |
| code  | code        |

#### Association: EmployeeProject

| GraphQL Field |
|---------------|
| assignee      |
