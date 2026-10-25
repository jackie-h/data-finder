# Legend Embedded (Otherwise) Mapping

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedMapping.pure` (`eDB` and the
`testMappingEmbeddedOtherwise` mapping). `Product.bondDetails.description` is embedded in the product row; the other
properties of `bondDetails` fall back to the join to BOND_DETAIL (Legend's `Otherwise`).

## Model: embedded_model.md

## DataStore: eDB (Database)

### Schema: main

#### Table: PRODUCT_DENORM → Product

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| PRODUCT_ID | INT | PK | N | bondDetails |
| PRODUCT_NAME | VARCHAR |  | Y | name |
| MARKET_NAME | VARCHAR |  | Y | market |
| BOND_DETAILS | VARCHAR |  | Y | bondDetails.description |

#### Association: ProductBondDetails

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PRODUCT_ID    | BOND_DETAIL  | BOND_ID       |

#### Table: BOND_DETAIL → BondDetail

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| BOND_ID | INT | PK | N | bondId |
| NOT_SO_GOOD_DETAIL | VARCHAR |  | Y | description |
| TYPE | VARCHAR |  | Y | type |
| HOLDERID | INT | FK | Y | holder |

#### Association: BondDetailHolder

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| HOLDERID      | Party        | id            |

#### Table: Party → Party

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | partyId |
| name | VARCHAR |  | Y | name |
