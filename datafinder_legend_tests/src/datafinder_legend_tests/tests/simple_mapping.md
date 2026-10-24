# Legend Simple Relational Mapping

Source: legend-engine core_relational `tests/relationalSetUp.pure` (`dbInc` tables and `simpleRelationalMappingInc`, the Person, Firm, Address, Location subset).

## Model: simple_model.md

## DataStore: dbInc (Database)

### Schema: default

#### Table: personTable → Person

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| FIRSTNAME | VARCHAR |     | firstName   |
| LASTNAME  | VARCHAR |     | lastName    |
| AGE       | INT     |     | age         |
| ADDRESSID | INT     | FK  | address     |
| FIRMID    | INT     | FK  | firm        |
| MANAGERID | INT     | FK  | manager     |

#### Association: PersonAddress

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID     | addressTable | ID            |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FIRMID        | firmTable    | ID            |

#### Association: PersonManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| MANAGERID     | personTable  | ID            |

#### Table: firmTable → Firm

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| LEGALNAME | VARCHAR |     | legalName   |
| ADDRESSID | INT     | FK  | address     |

#### Association: FirmAddress

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID     | addressTable | ID            |

#### Table: addressTable → Address

| Column   | Type    | Key | Property ID |
|----------|---------|-----|-------------|
| ID       | INT     | PK  | id          |
| NAME     | VARCHAR |     | name        |
| STREET   | VARCHAR |     | street      |
| COMMENTS | VARCHAR |     | comments    |

#### Table: locationTable → Location

| Column   | Type    | Key | Property ID |
|----------|---------|-----|-------------|
| ID       | INT     | PK  | id          |
| PERSONID | INT     | FK  | person      |
| PLACE    | VARCHAR |     | place       |
| date     | DATE    |     | censusdate  |

#### Association: PersonLocations

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PERSONID      | personTable  | ID            |
