# Finance Trade Model

## Sub-Domain: finance_trade

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Id         | Type       | Key | Description                           |
|------------|------------|------------|-----|---------------------------------------|
| Symbol     | symbol     | String     | Y   | The symbol of the instrument traded   |
| Price      | price      | Double     |     | The price at which trade was executed |
| Is Settled | is_settled | Boolean    |     |                                       |
| Account    | account    | Account    |     | The trading account                   |
| Instrument | instrument | Instrument |     |                                       |
| Valid From | valid_from | DateTime   |     |                                       |
| Valid To   | valid_to   | DateTime   |     |                                       |

### Association: TradeAccount

| Name         | Source | Target  | Description                  |
|--------------|--------|---------|------------------------------|
| TradeAccount | Trade  | Account | Links a trade to its account |
