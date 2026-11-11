# Legend Advanced Join Mapping

Source: legend-engine core_relational `tests/mapping/join/advancedRelationalSetUp.pure` (`advancedRelationalMapping`: Person
and Firm; not its extra-information and type properties). The join `Firm_Person` has conditions beside its key columns:
`firmTable.ID = personTable.FIRMID and firmTable.FLAG is null and personTable.FLAG is not null`.

## Model: advanced_join_model.md

## DataStore: db (Database)

### Schema: main

#### Table: personTable → Person

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| FIRSTNAME | VARCHAR |     | Y        | firstName   |
| LASTNAME  | VARCHAR |     | Y        | lastName    |
| FIRMID    | INT     | FK  | Y        |             |
| FLAG      | INT     |     | Y        |             |

#### Association: FirmPerson

| Source Column | Target Table | Target Column | Condition                                                |
|---------------|--------------|---------------|----------------------------------------------------------|
| FIRMID        | firmTable    | ID            | firmTable.FLAG IS NULL AND personTable.FLAG IS NOT NULL |

#### Table: firmTable → Firm

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| LEGALNAME | VARCHAR |     | Y        | legalName   |
| FLAG      | INT     |     | Y        |             |
