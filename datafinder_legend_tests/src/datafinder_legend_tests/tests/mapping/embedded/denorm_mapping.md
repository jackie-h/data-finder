# Legend Denormalised Mapping

Source: legend-engine core_relational `tests/mapping/embedded/testEmbeddedMapping.pure` (`myDB.PERSON_FIRM_DENORM` and the
`testMappingEmbedded` mapping). The firm and both addresses are held in the person's row, with no table to fall back to.

## Model: denorm_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PERSON_FIRM_DENORM → Person

| Column              | Type    | Key | Nullable | Property ID       |
|---------------------|---------|-----|----------|-------------------|
| PERSON_ID           | INT     | PK  | N        | personId          |
| PERSON_FIRSTNAME    | VARCHAR |     | Y        | firstName         |
| PERSON_LASTNAME     | VARCHAR |     | Y        | lastName          |
| PERSON_ADDRESS_NAME | VARCHAR |     | Y        | address.name      |
| FIRM_LEGALNAME      | VARCHAR |     | Y        | firm.legalName    |
| FIRM_ADDRESS_NAME   | VARCHAR |     | Y        | firm.address.name |
