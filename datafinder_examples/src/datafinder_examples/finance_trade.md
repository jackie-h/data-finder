# Finance Trade Model

## Sub-Domain: finance.trade

### Class: Trade

| Name  | Description                    |
|-------|--------------------------------|
| Trade | A trade executed on an account |

| Property   | Id         | Type       | Multiplicity | Key | Description                           |
|------------|------------|------------|--------------|-----|---------------------------------------|
| Symbol     | symbol     | String     | 1            | Y   | The symbol of the instrument traded   |
| Price      | price      | Double     | 1            |     | The price at which trade was executed |
| Is Settled | isSettled  | Boolean    | 1            |     |                                       |
| Valid From | validFrom  | DateTime   | 1            |     |                                       |
| Valid To   | validTo    | DateTime   | 1            |     |                                       |

### Class: ContractualPosition

| Name               | Description                                             |
|--------------------|---------------------------------------------------------|
| ContractualPosition | A position on a given business date as of a processing time |

| Property      | Id            | Type     | Multiplicity | Key | Description              |
|---------------|---------------|----------|--------------|-----|--------------------------|
| Business Date | businessDate  | Date     | 1            |     | The business date        |
| Quantity      | quantity      | Double   | 1            |     | Net quantity of position |
| Npv           | npv           | Double   | 1            |     | Net present value        |
| Valid From    | validFrom     | DateTime | 1            |     |                          |
| Valid To      | validTo       | DateTime | 1            |     |                          |

### Association: TradeAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|------------------------------|
| TradeAccount | Trade  | Trades               | trades             | *                   | Account | Account              | account            | 1                   | Links a trade to its account |
