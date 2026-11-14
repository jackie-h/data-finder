# SQL Query Sets Model

## Sub-Domain: org.sets

### Class: Customer

| Name     | Description                                                 |
|----------|-------------------------------------------------------------|
| Customer | A customer, who is a retail customer or a corporate one     |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: Order

| Name  | Description |
|-------|-------------|
| Order | An order    |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Id       | id     | Integer | 1            | Y   |             |
| Amount   | amount | Integer | 1            |     |             |

### Association: CustomerOrders

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| CustomerOrders | Customer | Orders               | orders             | *                   | Order  |                      |                    | *                   |             |
