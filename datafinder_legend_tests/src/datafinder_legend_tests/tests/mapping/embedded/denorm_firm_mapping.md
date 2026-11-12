# Legend Denormalised With Firm Distinct Mapping

Source: legend-engine core_relational `tests/mapping/embedded/testEmbeddedMapping.pure` (`testMappingEmbeddedWithFirmDistinct`).
The person's row holds the person and its address, as in denorm_mapping.md; the firms are the distinct firms of those rows, a query,
and a firm reaches its employees through the join on FIRM_ID.

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
| FIRM_ID             | INT     | PK  | N        |                   |
| FIRM_LEGALNAME      | VARCHAR |     | Y        |                   |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FIRM_ID       | FIRM_VIEW    | FIRM_ID       |

#### Query: FIRM_VIEW → Firm

```sql
SELECT DISTINCT FIRM_LEGALNAME, FIRM_ID
FROM PERSON_FIRM_DENORM
```

| Column         | Type    | Key | Nullable | Property ID |
|----------------|---------|-----|----------|-------------|
| FIRM_LEGALNAME | VARCHAR |     | Y        | legalName   |
| FIRM_ID        | INT     | PK  | N        |             |
