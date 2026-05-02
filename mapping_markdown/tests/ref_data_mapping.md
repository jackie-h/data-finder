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
