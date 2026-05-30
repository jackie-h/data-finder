# Finance Catalog Mapping

## Model: ../../mapping_markdown/tests/finance.md
## Model: ../../mapping_markdown/tests/finance_trade.md

## DataStore: my_catalog (Catalog)

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to |
|-----------------|------------------|----------------|---------------|--------------------|------------------|
| processing_only | in_z             | out_z          |               |                    |                  |

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Property   |
|------------|-----------|-----|------------|
| sym        | VARCHAR   |     | symbol     |
| price      | DOUBLE    |     | price      |
| is_settled | BOOLEAN   |     | isSettled  |
| account_id | INT       | FK  | account    |
| in_z       | TIMESTAMP |     | validFrom  |
| out_z      | TIMESTAMP |     | validTo    |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
