# Finance GraphQL Mapping

## Model: finance.md
## Model: finance_trade.md

## Endpoint: http://localhost:4000/graphql (filter: where, sort: order_by, limit: limit)

### Query: accounts → Account

| Field | Property ID |
|-------|----------|
| id    | id       |
| name  | name     |

### Query: instruments → Instrument (milestone: processing, asOf)

| Field  | Property ID |
|--------|----------|
| symbol | symbol   |
| price  | price    |

### Query: contractualPositions → ContractualPosition (milestone: bitemporal, businessDate, asOf)

| Field         | Property ID   |
|---------------|---------------|
| businessDate  | businessDate  |
| quantity      | quantity      |
| npv           | npv           |

### Query: trades → Trade (milestone: bitemporal, businessDate, asOf)

| Field      | Property ID |
|------------|------------|
| symbol     | symbol     |
| price      | price      |
| isSettled  | isSettled  |
| validFrom  | validFrom  |
| validTo    | validTo    |

#### Association: TradeAccount

| GraphQL Field |
|---------------|
| account       |
