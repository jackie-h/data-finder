# Org Chart GraphQL Mapping

## Model: orgchart.md

## Endpoint: http://localhost:4000/graphql (filter: where, sort: order_by, limit: limit)

### Query: employees → Employee

| Field | Property ID |
|-------|-------------|
| id    | id          |
| name  | name        |

#### Association: EmployeeManager

| GraphQL Field |
|---------------|
| manager       |
