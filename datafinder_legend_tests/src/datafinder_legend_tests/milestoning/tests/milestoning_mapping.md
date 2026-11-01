# Legend Milestoning Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`milestoningmap`, the tables the bitemporal tests reach).
The Legend tables carry in_z/out_z (processing) and from_z/thru_z (business) range columns.

## Model: milestoning_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime   |
|---------------------|------------------|----------------|---------------|--------------------|------------------|---------------------|
| bitemporal          | in_z             | out_z          |               | from_z             | thru_z           | 9999-12-31 23:59:59 |
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31          |
| processing_only     | in_z             | out_z          |               |                    |                  | 9999-12-31 23:59:59 |

### Schema: main

#### Table: BiTemporalProductTable → BiTemporalProduct (milestoning: bitemporal)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| type | VARCHAR | FK | Y | biTemporalClassification |
| in_z | TIMESTAMP |  | Y | validFrom |
| out_z | TIMESTAMP |  | Y | validTo |
| from_z | TIMESTAMP |  | Y | businessFrom |
| thru_z | TIMESTAMP |  | Y | businessThru |

#### Association: BiTemporalProductClassificationLink

| Source Column | Target Table                       | Target Column |
|---------------|------------------------------------|---------------|
| type          | BiTemporalProductClassificationTable | type        |

#### Table: BiTemporalProductClassificationTable → BiTemporalProductClassification (milestoning: bitemporal)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| system | VARCHAR |  | Y | system |
| exchange | VARCHAR |  | Y | exchange |
| in_z | TIMESTAMP |  | Y | validFrom |
| out_z | TIMESTAMP |  | Y | validTo |
| from_z | TIMESTAMP |  | Y | businessFrom |
| thru_z | TIMESTAMP |  | Y | businessThru |

#### Association: BiTemporalProductClassificationSingle

| Source Column | Target Table               | Target Column |
|---------------|----------------------------|---------------|
| type          | ProductClassificationTable | type          |

#### Table: OrderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| prodFk | INT | FK | Y | biTemporalProduct |

#### Association: OrderBiTemporalProduct

| Source Column | Target Table          | Target Column |
|---------------|-----------------------|---------------|
| prodFk        | BiTemporalProductTable | id           |

#### Table: ProductTable → Product (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | biTemporalClassification |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Association: ProductBiTemporalClassification

| Source Column | Target Table                         | Target Column |
|---------------|--------------------------------------|---------------|
| type          | BiTemporalProductClassificationTable | type          |

#### Table: ProductClassificationTable → ProductClassification (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| type_description | VARCHAR |  | Y | description |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Table: TraderTable → Trader (milestoning: processing_only)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| kerberos | VARCHAR | PK | N | kerberos |
| in_z | TIMESTAMP |  | Y | validFrom |
| out_z | TIMESTAMP |  | Y | validTo |

#### Association: TraderLocation

| Source Column | Target Table           | Target Column |
|---------------|------------------------|---------------|
| kerberos      | BiTemporalLocationTable | kerberos     |

#### Table: BiTemporalLocationTable → Location (milestoning: bitemporal)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| kerberos | VARCHAR |  | Y |  |
| place | VARCHAR | PK | N | place |
| in_z | TIMESTAMP |  | Y | validFrom |
| out_z | TIMESTAMP |  | Y | validTo |
| from_z | TIMESTAMP |  | Y | businessFrom |
| thru_z | TIMESTAMP |  | Y | businessThru |
