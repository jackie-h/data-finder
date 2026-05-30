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
