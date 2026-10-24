# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

| Scheme                    | processing_start | processing_end | business_date | business_date_from | business_date_to |
|---------------------------|------------------|----------------|---------------|--------------------|------------------|
| processing_only           | in_z             | out_z          |               |                    |                  |
| business_date_processing  | in_z             | out_z          | DATE          |                    |                  |
| bitemporal                | in_z             | out_z          |               | DATE_FROM          | DATE_TO          |

### Schema: ref_data

#### Table: account_master → Account (milestoning: processing_only)

| Column    | Type      | Key | Nullable | Property ID |
|-----------|-----------|-----|----------|-------------|
| ID        | INT       | PK  | N        | id          |
| ACCT_NAME | VARCHAR   |     | Y        | name        |
| BRANCH_ID | INT       | FK  | Y        | branch      |
| in_z      | TIMESTAMP |     | Y        | validFrom   |
| out_z     | TIMESTAMP |     | Y        | validTo     |

#### Association: AccountBranch

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| BRANCH_ID     | branch       | ID            |

#### Table: branch → Branch

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| ID     | INT     | PK  | N        | id          |
| CITY   | VARCHAR |     | Y        | city        |

#### Table: price → Instrument (milestoning: processing_only)

| Column | Type      | Key | Nullable | Property ID |
|--------|-----------|-----|----------|------------|
| SYM    | VARCHAR   | PK  | N        | symbol     |
| PRICE  | DOUBLE    |     | Y        | price      |
| in_z   | TIMESTAMP |     | Y        | validFrom  |
| out_z  | TIMESTAMP |     | Y        | validTo    |

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

#### Table: contractualposition → ContractualPosition (milestoning: business_date_processing)

| Column   | Type      | Key | Nullable | Property ID   |
|----------|-----------|-----|----------|---------------|
| DATE     | DATE      |     | Y        | businessDate  |
| QUANTITY | DOUBLE    |     | Y        | quantity      |
| NPV      | DOUBLE    |     | Y        | npv           |
| in_z     | TIMESTAMP |     | Y        | validFrom     |
| out_z    | TIMESTAMP |     | Y        | validTo       |
