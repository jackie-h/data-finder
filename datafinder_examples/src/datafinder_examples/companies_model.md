# Companies Model

## Sub-Domain: companies

### Class: Company

| Name    | Description |
|---------|-------------|
| Company |             |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| Id       | id       | Integer | 1            | Y   |             |
| Name     | name     | String  | 1            |     |             |
| Category | category | String  | 0..1         |     |             |
| Embedding | embedding | Vector  | 0..1         |     | An embedding of the company, a vector of 3 numbers |
