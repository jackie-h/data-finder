# Finance Catalog Mapping

## Model: ../../datafinder_examples/src/datafinder_examples/finance.md
## Model: ../../datafinder_examples/src/datafinder_examples/finance_trade.md

## DataStore: my_catalog (Catalog)

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to |
|-----------------|------------------|----------------|---------------|--------------------|------------------|
| processing_only | in_z             | out_z          |               |                    |                  |

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|----------|
| ID        | INT     | PK  | N        | id       |
| ACCT_NAME | VARCHAR |     | Y        | name     |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Nullable | Property ID |
|------------|-----------|-----|----------|------------|
| sym        | VARCHAR   |     | Y        | symbol     |
| price      | DOUBLE    |     | Y        | price      |
| is_settled | BOOLEAN   |     | Y        | isSettled  |
| account_id | INT       | FK  | Y        | account    |
| in_z       | TIMESTAMP |     | Y        | validFrom  |
| out_z      | TIMESTAMP |     | Y        | validTo    |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
