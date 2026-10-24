### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|----------|
| ID        | INT     | PK  | N        | id       |
| ACCT_NAME | VARCHAR |     | Y        | name     |

#### Table: price → Instrument (milestoning: processing_only)

| Column | Type      | Key | Nullable | Property ID |
|--------|-----------|-----|----------|------------|
| SYM    | VARCHAR   | PK  | N        | symbol     |
| PRICE  | DOUBLE    |     | Y        | price      |
| in_z   | TIMESTAMP |     | Y        | validFrom  |
| out_z  | TIMESTAMP |     | Y        | validTo    |
