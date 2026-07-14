# Finance Model

## Sub-Domain: finance.reference_data

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Id   | Type    | Key | Description |
|----------|------|---------|-----|-------------|
| Id       | id   | Integer | Y   |             |
| Name     | name | String  |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property | Id     | Type   | Key | Description |
|----------|--------|--------|-----|-------------|
| Symbol   | symbol | String | Y   |             |
| Price    | price  | Double |     |             |

### Class: Branch

| Name   | Description                            |
|--------|-----------------------------------------|
| Branch | Branch office an account is managed by |

| Property | Id   | Type    | Key | Description |
|----------|------|---------|-----|-------------|
| Id       | id   | Integer | Y   |             |
| City     | city | String  |     |             |

### Association: AccountBranch

| Name          | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|---------------|---------|-----------------------|---------------------|----------------------|--------|-----------------------|---------------------|----------------------|-------------------------------|
| AccountBranch | Account | Accounts              | accounts            | *                    | Branch | Branch                | branch              | 1                    | Links an account to its branch |
