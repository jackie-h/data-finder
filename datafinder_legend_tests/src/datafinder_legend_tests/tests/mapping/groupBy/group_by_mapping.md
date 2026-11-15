# Legend Group By Mapping

Source: legend-engine core_relational `tests/mapping/groupBy/testGroupBy.pure` (`testMapping`: Position is `~groupBy` the account, the product and
the gsn of TRADE, with the sum of the quantity). It is a `Query` section here, and its product is the table PRODUCT, joined through the product id the query keeps.

## Model: group_by_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: TRADE

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| TRADE_ID   | INT     | PK  | N        |             |
| ACC_NUM    | INT     |     | Y        |             |
| GSN        | VARCHAR |     | Y        |             |
| PRODUCT_ID | INT     |     | Y        |             |
| QTY        | INT     |     | Y        |             |

#### Table: PRODUCT → Product

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| ID          | INT     | PK  | N        | id          |
| DESCRIPTION | VARCHAR |     | Y        | description |

#### Query: POSITION → Position

```sql
SELECT ACC_NUM, GSN, PRODUCT_ID, SUM(QTY) AS quantity
FROM TRADE
GROUP BY ACC_NUM, PRODUCT_ID, GSN
```

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| ACC_NUM    | INT     | PK  | Y        | acctNum     |
| GSN        | VARCHAR | PK  | Y        | gsn         |
| PRODUCT_ID | INT     | PK  | Y        |             |
| quantity   | BIGINT  |     | Y        | quantity    |

#### Association: PositionProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PRODUCT_ID    | PRODUCT      | ID            |
