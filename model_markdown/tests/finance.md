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
| Account    | account    | Account    |     | The trading account                   |
| Instrument | instrument | Instrument |     |                                       |

### Association: TradeAccount

| Name         | Source | Target  | Description                  |
|--------------|--------|---------|------------------------------|
| TradeAccount | Trade  | Account | Links a trade to its account |
