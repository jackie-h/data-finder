### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Nullable | Property ID |
|------------|-----------|-----|----------|-------------|
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
