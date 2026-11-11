# Legend Bitemporal Types On Unmilestoned Tables Mapping

Source: legend-engine core_relational `milestoning/tests/businessDateMilestoningSetUp.pure` (`noMilestoningMap`: Order,
BiTemporalProduct and BiTemporalProductClassification). The bitemporal types are mapped to
ProductTableNoMilestoning and ProductClassificationTableNoMilestoning, which have no milestoning columns, so the dates a
query gives them are ignored and nothing filters the tables.

## Model: bitemporal_none_model.md

## DataStore: db (Database)

### Schema: main

#### Table: OrderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| prodFk | INT | FK | Y | biTemporalProduct |

#### Association: OrderBiTemporalProduct

| Source Column | Target Table                | Target Column |
|---------------|-----------------------------|---------------|
| prodFk        | ProductTableNoMilestoning   | id            |

#### Table: ProductTableNoMilestoning → BiTemporalProduct

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| type | VARCHAR | FK | Y | biTemporalClassification |

#### Association: BiTemporalProductClassificationLink

| Source Column | Target Table                           | Target Column |
|---------------|----------------------------------------|---------------|
| type          | ProductClassificationTableNoMilestoning | type         |

#### Table: ProductClassificationTableNoMilestoning → BiTemporalProductClassification

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| type | VARCHAR | PK | N | type |
