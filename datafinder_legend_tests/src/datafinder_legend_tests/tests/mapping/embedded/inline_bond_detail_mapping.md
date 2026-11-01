# Legend Inline Embedded Bond Detail Mapping

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedNested.pure` and
`testInlineEmbeddedTargetIds.pure` (`eDB.PRODUCT_DENORM`). BondDetail is mapped onto the denormalised table with its issuer and
holder, and each party's address, in the same row. Legend writes the parties and addresses as inline class mappings with
their own ids; the two files give the same columns.

## Model: inline_model.md

## DataStore: eDB (Database)

### Schema: main

#### Table: PRODUCT_DENORM → BondDetail

| Column                | Type    | Key | Nullable | Property ID          |
|-----------------------|---------|-----|----------|----------------------|
| PRODUCT_ID            | INT     | PK  | N        | id                   |
| BOND_DETAILS          | VARCHAR |     | Y        | description          |
| BOND_TYPE             | VARCHAR |     | Y        | type                 |
| ISSUER                | VARCHAR |     | Y        | issuer.name          |
| ISSUER_ADDRESS_NAME   | VARCHAR |     | Y        | issuer.address.name  |
| ISSUER_ADDRESS_STREET | VARCHAR |     | Y        | issuer.address.street |
| HOLDER                | VARCHAR |     | Y        | holder.name          |
| HOLDER_ADDRESS_NAME   | VARCHAR |     | Y        | holder.address.name  |
| HOLDER_ADDRESS_STREET | VARCHAR |     | Y        | holder.address.street |
