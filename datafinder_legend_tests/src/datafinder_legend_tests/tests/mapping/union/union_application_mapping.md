# Legend Application Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`mappingWithColumnsWithSameNames`).
Application, Person and Firm are each a table, and the nick name of the person and of the firm are columns of the same name in different tables.

## Model: union_application_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ApplicationsSet1 → Application

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| date | DATE | PK | N | date |
| personID | INT | PK | N |  |
| firmID | INT | PK | N |  |

#### Association: ApplicationApplicant

| Source Column | Target Table | Target Column |
|---|---|---|
| personID | PersonSet1 | ID |

#### Association: ApplicationFirm

| Source Column | Target Table | Target Column |
|---|---|---|
| firmID | FirmSet1 | ID |

#### Table: PersonSet1 → Person

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| NICKNAME | VARCHAR |  | Y | nickName |

#### Table: FirmSet1 → Firm

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| NICKNAME | VARCHAR |  | Y | nickName |
