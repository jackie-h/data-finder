# Finance Mapping

## Model: finance.md

## Repository: finance_db

| Scheme          | processing_start | processing_end | business_date |
|-----------------|------------------|----------------|---------------|
| bitemporal      | in_z             | out_z          | business_date |
| processing_only | in_z             | out_z          |               |

### Schema: ref_data

#### Table: account_master → Account

| Column    | Property |
|-----------|----------|
| ID        | id       |
| ACCT_NAME | name     |

#### Table: price → Instrument (milestoning: processing_only)

| Column | Property   |
|--------|------------|
| SYM    | symbol     |
| PRICE  | price      |
| in_z   | valid_from |
| out_z  | valid_to   |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Property   |
|------------|------------|
| sym        | symbol     |
| price      | price      |
| account_id | account    |
| in_z       | valid_from |
| out_z      | valid_to   |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
