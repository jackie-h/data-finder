# Finance Model

## Sub-Domain: finance

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Type    | Key | Description |
|----------|---------|-----|-------------|
| id       | Integer | Y   |             |
| name     | String  |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property | Type   | Key | Description |
|----------|--------|-----|-------------|
| symbol   | String | Y   |             |
| price    | Double |     |             |

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property | Type       | Key | Description                         |
|----------|------------|-----|-------------------------------------|
| symbol   | String     | Y   | The symbol of the instrument traded |
| price    | Double     |     | The price at which trade was executed |
| account  | Account    |     | The trading account                 |
| instrument | Instrument |   |                                     |

### Association: TradeAccount

| Name         | Source | Target  | Description                  |
|--------------|--------|---------|------------------------------|
| TradeAccount | Trade  | Account | Links a trade to its account |
