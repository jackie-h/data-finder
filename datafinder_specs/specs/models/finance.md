# Finance Model

## Sub-Domain: finance.reference_data

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Symbol   | symbol | String | 1            | Y   |             |
| Price    | price  | Double | 1            |     |             |

### Class: Branch

| Name   | Description                            |
|--------|-----------------------------------------|
| Branch | Branch office an account is managed by |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| City     | city | String  | 0..1         |     |             |

### Class: Desk

| Name | Description                                  |
|------|----------------------------------------------|
| Desk | A trading desk that a group of accounts is part of |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Association: AccountBranch

| Name          | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description                    |
|---------------|---------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|--------------------------------|
| AccountBranch | Account | Branch               | branch             | 0..1                | Branch | Accounts             | accounts           | *                   | Links an account to its branch |

### Association: AccountDesk

| Name        | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description                          |
|-------------|---------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|--------------------------------------|
| AccountDesk | Account | Desk                 | desk               | 0..1                | Desk   | Accounts             | accounts           | *                   | Links an account to the desk it is part of |
