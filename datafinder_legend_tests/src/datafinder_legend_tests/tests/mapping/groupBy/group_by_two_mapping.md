# Legend Group By Mapping With Two Group Bys

Source: legend-engine core_relational `tests/mapping/groupBy/testGroupBy.pure` (`testMappingWithTwoGroupBysAndFilters`: Position is `~groupBy` the account, the product and
the gsn of the TRADE rows of product 1, with the sum of the quantity). It is a `Query` section here, and its product is a query of the active rows of PRODUCT_EXPLODED grouped by product and description, joined through the product id the query keeps.

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

#### Table: PRODUCT_EXPLODED

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| TRADEABLE_ID | INT     | PK  | N        |             |
| PRODUCT_ID   | INT     |     | Y        |             |
| DESCRIPTION  | VARCHAR |     | Y        |             |
| TRADEABLE    | VARCHAR |     | Y        |             |
| ACTIVE       | INT     |     | Y        |             |

#### Query: PRODUCT → Product

```sql
SELECT PRODUCT_ID, DESCRIPTION
FROM PRODUCT_EXPLODED
WHERE ACTIVE = 1
GROUP BY PRODUCT_ID, DESCRIPTION
```

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| PRODUCT_ID  | INT     | PK  | Y        | id          |
| DESCRIPTION | VARCHAR | PK  | Y        | description |

#### Query: POSITION → Position

```sql
SELECT ACC_NUM, GSN, PRODUCT_ID, SUM(QTY) AS quantity
FROM TRADE
WHERE PRODUCT_ID = 1
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
| PRODUCT_ID    | PRODUCT      | PRODUCT_ID    |
