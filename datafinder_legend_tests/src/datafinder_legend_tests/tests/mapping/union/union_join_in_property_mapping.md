# Legend Union Mapping: Join In Property

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithJoinInProperty`): in the first set the
last name is read through a join to PersonExtension, in the second it is its own column. (Person's otherNames, a function of it, is not in the model.)
The join is inside the first select of the union.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: Firm → Firm

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| ID     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        | legalName   |

#### Table: PersonSet1

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| ID           | INT     | PK  | N        |             |
| firstName_s1 | VARCHAR |     | Y        |             |
| lastName_s1  | VARCHAR |     | Y        |             |
| FirmID       | INT     |     | Y        |             |
| ADDRESSID    | INT     |     | Y        |             |

#### Table: PersonSet2

| Column       | Type    | Key | Nullable | Property ID |
|--------------|---------|-----|----------|-------------|
| ID           | INT     | PK  | N        |             |
| firstName_s2 | VARCHAR |     | Y        |             |
| lastName_s2  | VARCHAR |     | Y        |             |
| FirmID       | INT     |     | Y        |             |
| ADDRESSID    | INT     |     | Y        |             |

#### Table: PersonExtension

| Column        | Type    | Key | Nullable | Property ID |
|---------------|---------|-----|----------|-------------|
| ID            | INT     | PK  | N        |             |
| otherLastName | VARCHAR |     | Y        |             |

#### Query: PERSON_UNION → Person

```sql
SELECT x.otherLastName AS lastName, s.FirmID
FROM PersonSet1 s
LEFT OUTER JOIN PersonExtension x ON s.ID = x.ID
UNION ALL
SELECT lastName_s2 AS lastName, FirmID FROM PersonSet2
```

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| lastName   | VARCHAR |     | Y        | lastName    |
| FirmID     | INT     |     | Y        |             |


#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FirmID        | Firm         | ID            |
