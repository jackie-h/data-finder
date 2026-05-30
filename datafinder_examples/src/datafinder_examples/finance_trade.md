# Finance Trade Model

## Sub-Domain: finance.trade

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Id         | Type       | Key | Description                           |
|------------|------------|------------|-----|---------------------------------------|
| Symbol     | symbol     | String     | Y   | The symbol of the instrument traded   |
| Price      | price      | Double     |     | The price at which trade was executed |
| Is Settled | isSettled  | Boolean    |     |                                       |
| Valid From | validFrom  | DateTime   |     |                                       |
| Valid To   | validTo    | DateTime   |     |                                       |

### Class: ContractualPosition

| Name               | Description                                             |
|--------------------|---------------------------------------------------------|
| ContractualPosition | A position on a given business date as of a processing time |

| Property      | Id            | Type     | Key | Description              |
|---------------|---------------|----------|-----|--------------------------|
| Business Date | businessDate  | Date     |     | The business date        |
| Quantity      | quantity      | Double   |     | Net quantity of position |
| Npv           | npv           | Double   |     | Net present value        |
| Valid From    | validFrom     | DateTime |     |                          |
| Valid To      | validTo       | DateTime |     |                          |

### Association: TradeAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|------------------------------|
| TradeAccount | Trade  | Trades               | trades             | *                   | Account | Account              | account            | 1                   | Links a trade to its account |
