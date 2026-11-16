# Legend Milestoning Propagation Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`propagationMapping`, the Product and NewActivityInfo tables).
A product's rows are milestoned, and its new activity is a row of NewActivityInfoTable, a table with from_z and thru_z that its class does not use, so the date of the product
is not applied to it. Its cancel activity is reached through CancelActivitiesTable, which is milestoned like the product but is not a class: the date is applied to it
where it is passed through, and not to the activity it reaches.

## Model: propagation_model.md

## DataStore: db (Database)

| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

### Schema: main

#### Table: ProductTable → Product (milestoning: business_date_range)

| Column | Type | Key | Nullable | Property ID  |
|--------|------|-----|----------|--------------|
| id     | INT  | PK  | N        | id           |
| from_z | DATE |     | Y        | businessFrom |
| thru_z | DATE |     | Y        | businessThru |

#### Table: NewActivityInfoTable → NewActivityInfo

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| productId  | INT     | PK  | N        |             |
| created_by | VARCHAR |     | Y        | createdBy   |
| from_z     | DATE    |     | Y        |             |
| thru_z     | DATE    |     | Y        |             |

#### Table: CancelActivitiesTable (milestoning: business_date_range)

| Column    | Type | Key | Nullable | Property ID |
|-----------|------|-----|----------|-------------|
| productId | INT  | PK  | N        |             |
| from_z    | DATE |     | Y        |             |
| thru_z    | DATE |     | Y        |             |

#### Association: ProductNewActivity

| Source Column | Target Table        | Target Column |
|---------------|---------------------|---------------|
| id            | NewActivityInfoTable | productId    |

#### Association: ProductCancelActivity

| Source Column | Target Table          | Target Column |
|---------------|-----------------------|---------------|
| id            | CancelActivitiesTable | productId     |
| productId     | NewActivityInfoTable  | productId     |
