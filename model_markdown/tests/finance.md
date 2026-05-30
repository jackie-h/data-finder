# Finance Model

## Sub-Domain: finance

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Id      | Type    | Key | Description |
|----------|---------|---------|-----|-------------|
| Id       | id      | Integer | Y   |             |
| Name     | name    | String  |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property | Id     | Type   | Key | Description |
|----------|--------|--------|-----|-------------|
| Symbol   | symbol | String | Y   |             |
| Price    | price  | Double |     |             |

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Id         | Type       | Key | Description                           |
|------------|------------|------------|-----|---------------------------------------|
| Symbol     | symbol     | String     | Y   | The symbol of the instrument traded   |
| Price      | price      | Double     |     | The price at which trade was executed |

### Association: TradeAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|------------------------------|
| TradeAccount | Trade  | Trades               | trades             | *                   | Account | Account              | account            | 1                   | Links a trade to its account |
