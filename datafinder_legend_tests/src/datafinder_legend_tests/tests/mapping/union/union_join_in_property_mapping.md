# Legend Union With Join In Property Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithJoinInProperty`).
In the first set the last name is read through a join to PersonExtension, on its key, inside the query of the set; in the second it is its own column. (Person's otherNames, a function of it, is not in the model.)

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonSet1

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| firstName_s1 | VARCHAR |  | Y |  |
| lastName_s1 | VARCHAR |  | Y |  |
| FirmID | INT |  | Y |  |
| ADDRESSID | INT |  | Y |  |
| NICKNAME | VARCHAR |  | Y |  |

#### Table: PersonSet2

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| firstName_s2 | VARCHAR |  | Y |  |
| lastName_s2 | VARCHAR |  | Y |  |
| FirmID | INT |  | Y |  |
| ADDRESSID | INT |  | Y |  |

#### Table: PersonExtension

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| otherLastName | VARCHAR |  | Y |  |

#### Table: Firm → Firm

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| name | VARCHAR |  | Y | legalName |

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT s.ID, x.otherLastName AS lastName, s.FirmID
FROM PersonSet1 s
LEFT OUTER JOIN PersonExtension x ON s.ID = x.ID
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| FirmID | INT |  | Y |  |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT ID, lastName_s2 AS lastName, FirmID FROM PersonSet2
```

| Column | Type | Key | Nullable | Property ID |
|---|---|---|---|---|
| ID | INT | PK | N |  |
| lastName | VARCHAR |  | Y | lastName |
| FirmID | INT |  | Y |  |

#### Association: PersonFirm (source set: set1)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | Firm | ID |

#### Association: PersonFirm (source set: set2)

| Source Column | Target Table | Target Column |
|---|---|---|
| FirmID | Firm | ID |
