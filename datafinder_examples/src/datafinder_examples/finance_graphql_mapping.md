# Finance GraphQL Mapping

## Model: finance.md
## Model: finance_trade.md

## Endpoint: http://localhost:4000/graphql (filter: where, sort: order_by, limit: limit)

### Query: accounts → Account (milestone: processing, asOf)

| Field | Property ID |
|-------|----------|
| id    | id       |
| name  | name     |

#### Association: AccountBranch

| GraphQL Field |
|---------------|
| branch        |

#### Association: AccountDesk

| GraphQL Field |
|---------------|
| desk          |

#### Association: PositionAccount

| GraphQL Field |
|---------------|
| positions     |

### Query: branches → Branch

| Field | Property ID |
|-------|----------|
| id    | id       |
| city  | city     |

#### Association: AccountBranch

| GraphQL Field |
|---------------|
| accounts      |

### Query: desks → Desk

| Field | Property ID |
|-------|----------|
| id    | id       |
| name  | name     |

#### Association: AccountDesk

| GraphQL Field |
|---------------|
| accounts      |

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

#### Association: PositionAccount

| GraphQL Field |
|---------------|
| account       |

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

#### Association: TradeInstrument

| GraphQL Field |
|---------------|
| instrument    |
