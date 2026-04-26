# Finance Trade Model

## Sub-Domain: finance_trade

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Type       | Key | Description                           |
|------------|------------|-----|---------------------------------------|
| symbol      | String     | Y   | The symbol of the instrument traded   |
| price       | Double     |     | The price at which trade was executed |
| is_settled  | Boolean    |     |                                       |
| account     | Account    |     | The trading account                   |
| instrument  | Instrument |     |                                       |
| valid_from  | DateTime   |     |                                       |
| valid_to    | DateTime   |     |                                       |

### Association: TradeAccount

| Name         | Source | Target  | Description                  |
|--------------|--------|---------|------------------------------|
| TradeAccount | Trade  | Account | Links a trade to its account |
