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
