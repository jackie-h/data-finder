# Finance Model

## Sub-Domain: finance

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Id      | Type    | Multiplicity | Key | Description |
|----------|---------|---------|--------------|-----|-------------|
| Id       | id      | Integer | 1            | Y   |             |
| Name     | name    | String  | 1            |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Symbol   | symbol | String | 1            | Y   |             |
| Price    | price  | Double | 1            |     |             |

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Id         | Type       | Multiplicity | Key | Description                           |
|------------|------------|------------|--------------|-----|---------------------------------------|
| Symbol     | symbol     | String     | 1            | Y   | The symbol of the instrument traded   |
| Price      | price      | Double     | 1            |     | The price at which trade was executed |

### Association: TradeAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|------------------------------|
| TradeAccount | Trade  | Account              | account            | 1                   | Account | Trades               | trades             | *                   | Links a trade to its account |
