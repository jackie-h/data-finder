# Positions Mapping

## Model: positions_model.md

## DataStore: positions_db (Database)

### Schema: main

#### Table: positions → Position

| Column     | Type   | Key | Nullable | Property ID |
|------------|--------|-----|----------|------------|
| id         | INT    | PK  | N        | id         |
| trade_date | DATE   |     | Y        | tradeDate  |
| npv        | DOUBLE |     | Y        | npv        |
