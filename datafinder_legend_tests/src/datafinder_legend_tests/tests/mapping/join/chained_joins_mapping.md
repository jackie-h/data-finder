# Legend Chained Joins Mapping

Source: legend-engine core_relational `tests/mapping/join/advancedRelationalSetUp.pure` (`chainedJoins`: Firm.employees, which
goes `@Firm_FirmPersonBridge > @Person_FirmPersonBridge`; not Person.firm, which goes the other way).

## Model: chained_joins_model.md

## DataStore: db (Database)

### Schema: main

#### Table: firmTable → Firm

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| LEGALNAME | VARCHAR |     | Y        | legalName   |

#### Association: FirmEmployees

| Source Column | Target Table          | Target Column |
|---------------|-----------------------|---------------|
| ID            | firmPersonBridgeTable | FIRM_ID       |
| PERSON_ID     | personTable           | ID            |

#### Table: firmPersonBridgeTable

| Column    | Type | Key | Nullable | Property ID |
|-----------|------|-----|----------|-------------|
| FIRM_ID   | INT  |     | Y        |             |
| PERSON_ID | INT  |     | Y        |             |

#### Table: personTable → Person

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| FIRSTNAME | VARCHAR |     | Y        | firstName   |
| LASTNAME  | VARCHAR |     | Y        | lastName    |
