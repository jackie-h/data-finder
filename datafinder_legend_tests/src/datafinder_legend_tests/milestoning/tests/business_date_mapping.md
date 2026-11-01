# Legend Business Date Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`db`, the Order, Product,
ProductClassification, ProductSynonym and Exchange tables). Product, ProductClassification, ProductSynonym and Exchange rows apply from `from_z` up to
but not including `thru_z`, and the infinity date is 9999-12-31. Order has no milestoning.

## Model: business_date_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

### Schema: main

#### Table: OrderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| prodFk | INT | FK | Y | product |

#### Association: OrderProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodFk        | ProductTable | id            |

#### Table: ProductTable → Product (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | classification |
| exchange | VARCHAR | FK | Y | exchange |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Association: ProductClassificationLink

| Source Column | Target Table         | Target Column |
|---------------|----------------------|---------------|
| type          | ProductClassificationTable | type    |

#### Association: ProductSynonymLink

| Source Column | Target Table        | Target Column |
|---------------|---------------------|---------------|
| name          | ProductSynonymTable | name          |

#### Association: ProductExchangeLink

| Source Column | Target Table         | Target Column |
|---------------|----------------------|---------------|
| exchange      | ProductExchangeTable | name          |

#### Table: ProductClassificationTable → ProductClassification (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
| type_description | VARCHAR |  | Y | description |
| exchange | VARCHAR | FK | Y | exchange |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Association: ClassificationExchangeLink

| Source Column | Target Table         | Target Column |
|---------------|----------------------|---------------|
| exchange      | ProductExchangeTable | name          |

#### Table: ProductSynonymTable → ProductSynonym (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| name | VARCHAR |  | Y |  |
| synonym | VARCHAR | PK | N | synonym |
| type | VARCHAR |  | Y | type |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |

#### Table: ProductExchangeTable → Exchange (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| name | VARCHAR | PK | N | name |
| from_z | DATE |  | Y | businessFrom |
| thru_z | DATE |  | Y | businessThru |
