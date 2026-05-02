# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## Repository: finance_db

| Scheme          | processing_start | processing_end | business_date |
|-----------------|------------------|----------------|---------------|
| bitemporal      | in_z             | out_z          | business_date |
| processing_only | in_z             | out_z          |               |

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

#### Table: price → Instrument (milestoning: processing_only)

| Column | Type      | Key | Property   |
|--------|-----------|-----|------------|
| SYM    | VARCHAR   | PK  | symbol     |
| PRICE  | DOUBLE    |     | price      |
| in_z   | TIMESTAMP |     | valid_from |
| out_z  | TIMESTAMP |     | valid_to   |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Property   |
|------------|-----------|-----|------------|
| sym        | VARCHAR   |     | symbol     |
| price      | DOUBLE    |     | price      |
| is_settled | BOOLEAN   |     | is_settled |
| account_id | INT       | FK  | account    |
| in_z       | TIMESTAMP |     | valid_from |
| out_z      | TIMESTAMP |     | valid_to   |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
