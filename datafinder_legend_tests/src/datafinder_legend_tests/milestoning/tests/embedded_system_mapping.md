# Legend Embedded System Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure`
(`milestoningMapWithEmbeddedSimple`, without the inline exchange). A product's classification is held in its row: its
type is the `type` column, and its system is reached from the `classificationSystemId` column through
ProductClassificationSystemTable (milestoned the same way as the product) to SystemTable.

## Model: embedded_system_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

### Schema: main

#### Table: ProductTable → Product (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR |  | Y | classification.type |
| classificationSystemId | INT | FK | Y | classification.system |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Table: ProductClassificationSystemTable (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N |  |
| name | VARCHAR |  | Y |  |
| from_z | DATE |  | Y |  |
| thru_z | DATE |  | Y |  |

#### Table: SystemTable → System

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| name | VARCHAR | PK | N | name |

#### Association: ProductClassificationSystem (embedded in Product.classification)

| Source Column          | Target Table                     | Target Column |
|------------------------|----------------------------------|---------------|
| classificationSystemId | ProductClassificationSystemTable | id            |
| name                   | SystemTable                      | name          |
