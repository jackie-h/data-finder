# Legend Inline Embedded Product Mapping

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedMapping.pure` (`eDB.PRODUCT_DENORM` and the
`testMappingEmbedded` mapping). Product is mapped onto the denormalised table and its bond details are held in the same row,
which Legend writes as `bondDetails() Inline[myBondMapping]`.

## Model: inline_model.md

## DataStore: eDB (Database)

### Schema: main

#### Table: PRODUCT_DENORM → Product

| Column       | Type    | Key | Nullable | Property ID         |
|--------------|---------|-----|----------|---------------------|
| PRODUCT_ID   | INT     | PK  | N        | id                  |
| PRODUCT_NAME | VARCHAR |     | Y        | name                |
| MARKET_NAME  | VARCHAR |     | Y        | market              |
| BOND_DETAILS | VARCHAR |     | Y        | bondDetails.description |
| BOND_TYPE    | VARCHAR |     | Y        | bondDetails.type    |
