# Legend Union With Embedded Properties Of The Same Column Names

Source: legend-engine core_relational `tests/mapping/union/testUnion.pure` (`unionMappingWithEmbeddedPropertyWithColumnsWithSameNames`).
Application is the union of two sets, a query for each of ApplicationsSet1 and ApplicationsSet2, and its applicant, its firm and their addresses are embedded in the row of
each set. The tables name their columns differently (`nickName_s1`, `nickName`); each set gives them the same names. A set selects the key of its table: the date, the person
and the firm for the first, and the date, the name of the person and the legal name of the firm for the second, so each gives the columns of the other as a placeholder (Legend pads with null; a query cannot give a null yet, and these are not properties).
The firm of the second set is the constant `'vogue'`, and its key is the legal name column, which is not the property.

## Model: union_application_embedded_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: ApplicationsSet1

| Column            | Type    | Key | Nullable | Property ID |
|-------------------|---------|-----|----------|-------------|
| date              | DATE    | PK  | N        |             |
| personID          | INT     | PK  | N        |             |
| firstName_s1      | VARCHAR |     | Y        |             |
| lastName_s1       | VARCHAR |     | Y        |             |
| nickName_s1       | VARCHAR |     | Y        |             |
| person_address_s1 | VARCHAR |     | Y        |             |
| firmID            | INT     | PK  | N        |             |
| firm_legalName_s1 | VARCHAR |     | Y        |             |
| firm_nickName_s1  | VARCHAR |     | Y        |             |
| firm_address_s1   | VARCHAR |     | Y        |             |

#### Table: ApplicationsSet2

| Column            | Type    | Key | Nullable | Property ID |
|-------------------|---------|-----|----------|-------------|
| date              | DATE    | PK  | N        |             |
| firstName         | VARCHAR | PK  | N        |             |
| lastName          | VARCHAR | PK  | N        |             |
| nickName          | VARCHAR |     | Y        |             |
| person_address_s2 | VARCHAR |     | Y        |             |
| firm_legalName    | VARCHAR | PK  | N        |             |
| firm_nickName     | VARCHAR |     | Y        |             |
| firm_address_s2   | VARCHAR |     | Y        |             |

#### Query: APPLICATIONS_ONE → Application (set: set1)

```sql
SELECT date, personID AS person_id, firmID AS firm_id, 'agency1' AS source,
       firstName_s1 AS applicant_first_name, lastName_s1 AS applicant_last_name, nickName_s1 AS applicant_nick_name,
       person_address_s1 AS applicant_address_name,
       firm_legalName_s1 AS firm_legal_name, firm_nickName_s1 AS firm_nick_name, firm_address_s1 AS firm_address_name,
       firm_legalName_s1 AS firm_key
FROM ApplicationsSet1
```

| Column                 | Type    | Key | Nullable | Property ID             |
|------------------------|---------|-----|----------|-------------------------|
| date                   | DATE    | PK  | N        | date                    |
| person_id              | INT     | PK  | N        |                         |
| firm_id                | INT     | PK  | N        |                         |
| source                 | VARCHAR |     | Y        | source                  |
| applicant_first_name   | VARCHAR |     | Y        | applicant.firstName     |
| applicant_last_name    | VARCHAR |     | Y        | applicant.lastName      |
| applicant_nick_name    | VARCHAR |     | Y        | applicant.nickName      |
| applicant_address_name | VARCHAR |     | Y        | applicant.address.name  |
| firm_legal_name        | VARCHAR |     | Y        | firm.legalName          |
| firm_nick_name         | VARCHAR |     | Y        | firm.nickName           |
| firm_address_name      | VARCHAR |     | Y        | firm.address.name       |
| firm_key               | VARCHAR |     | Y        |                         |

#### Query: APPLICATIONS_TWO → Application (set: set2)

```sql
SELECT date, 0 AS person_id, 0 AS firm_id, 'agency2' AS source,
       firstName AS applicant_first_name, lastName AS applicant_last_name, nickName AS applicant_nick_name,
       person_address_s2 AS applicant_address_name,
       'vogue' AS firm_legal_name, firm_nickName AS firm_nick_name, firm_address_s2 AS firm_address_name,
       firm_legalName AS firm_key
FROM ApplicationsSet2
```

| Column                 | Type    | Key | Nullable | Property ID             |
|------------------------|---------|-----|----------|-------------------------|
| date                   | DATE    | PK  | N        | date                    |
| person_id              | INT     |     | Y        |                         |
| firm_id                | INT     |     | Y        |                         |
| source                 | VARCHAR |     | Y        | source                  |
| applicant_first_name   | VARCHAR | PK  | N        | applicant.firstName     |
| applicant_last_name    | VARCHAR | PK  | N        | applicant.lastName      |
| applicant_nick_name    | VARCHAR |     | Y        | applicant.nickName      |
| applicant_address_name | VARCHAR |     | Y        | applicant.address.name  |
| firm_legal_name        | VARCHAR |     | Y        | firm.legalName          |
| firm_nick_name         | VARCHAR |     | Y        | firm.nickName           |
| firm_address_name      | VARCHAR |     | Y        | firm.address.name       |
| firm_key               | VARCHAR | PK  | N        |                         |
