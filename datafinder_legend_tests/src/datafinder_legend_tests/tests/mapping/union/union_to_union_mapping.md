# Legend Union To Union Mapping

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionToUnionMapping`): Person is the union of PersonSet1 and
PersonSet2, and Firm the union of FirmSet1 and FirmSet2, joined through each pair of sets' FirmID. Each union is a query here, and a
person reaches its firm through the one join between the two queries.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: FirmSet1

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |
| NICKNAME | VARCHAR |   | Y        |             |

#### Table: FirmSet2

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

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

#### Query: PERSON_UNION → Person

```sql
SELECT lastName_s1 AS lastName, FirmID FROM PersonSet1
UNION ALL
SELECT lastName_s2 AS lastName, FirmID FROM PersonSet2
```

| Column   | Type    | Key | Nullable | Property ID |
|----------|---------|-----|----------|-------------|
| lastName | VARCHAR |     | Y        | lastName    |
| FirmID   | INT     |     | Y        |             |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FirmID        | FIRM_UNION   | id            |

#### Query: FIRM_UNION → Firm

```sql
SELECT id, name FROM FirmSet1
UNION ALL
SELECT id, name FROM FirmSet2
```

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     |     | Y        |             |
| name   | VARCHAR |     | Y        | legalName   |
