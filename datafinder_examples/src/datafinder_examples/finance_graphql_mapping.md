# Finance GraphQL Mapping

## Model: finance.md
## Model: finance_trade.md

## Endpoint: http://localhost:4000/graphql

### Query: accounts → Account

| Field | Property |
|-------|----------|
| id    | id       |
| name  | name     |

### Query: instruments → Instrument (milestone: processing, asOf)

| Field  | Property |
|--------|----------|
| symbol | symbol   |
| price  | price    |

### Query: contractualPositions → ContractualPosition (milestone: business_date, businessDate)

| Field         | Property      |
|---------------|---------------|
| businessDate  | businessDate  |
| quantity      | quantity      |
| npv           | npv           |

### Query: trades → Trade (milestone: bitemporal, businessDate, asOf)

| Field      | Property   |
|------------|------------|
| symbol     | symbol     |
| price      | price      |
| isSettled  | isSettled  |
