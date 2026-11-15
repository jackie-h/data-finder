# SQL Query Sets Embedded Mapping

An application is the union of two sets, a query for each table, and the applicant, the firm and the applicant's address are embedded in each: they are read
from the columns of the set's own row. A column of a set maps to the property of an embedded object by a dotted Property ID (`applicant.address.name`), as a
column of a table does, and the sets have to give the same embedded objects from the same columns, as they give the same properties. A constant is a column
of the query: `'agency1' AS source`.

## Model: sql_query_sets_embedded_model.md

## DataStore: org_db (Database)

### Schema: org

#### Table: applications_a

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| id         | INT     | PK  | N        |             |
| first_name | VARCHAR |     | Y        |             |
| nick_name  | VARCHAR |     | Y        |             |
| home       | VARCHAR |     | Y        |             |
| firm_name  | VARCHAR |     | Y        |             |

#### Table: applications_b

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| id          | INT     | PK  | N        |             |
| given_name  | VARCHAR |     | Y        |             |
| known_as    | VARCHAR |     | Y        |             |
| address     | VARCHAR |     | Y        |             |

#### Query: AGENCY_ONE → Application (set: agency_one)

```sql
SELECT id, 'agency1' AS source, first_name AS applicant_first_name, nick_name AS applicant_nick_name,
       home AS applicant_home, firm_name AS firm_legal_name
FROM applications_a
```

| Column               | Type    | Key | Nullable | Property ID             |
|----------------------|---------|-----|----------|-------------------------|
| id                   | INT     | PK  | N        | id                      |
| source               | VARCHAR |     | Y        | source                  |
| applicant_first_name | VARCHAR |     | Y        | applicant.firstName     |
| applicant_nick_name  | VARCHAR |     | Y        | applicant.nickName      |
| applicant_home       | VARCHAR |     | Y        | applicant.address.name  |
| firm_legal_name      | VARCHAR |     | Y        | firm.legalName          |

#### Query: AGENCY_TWO → Application (set: agency_two)

```sql
SELECT id, 'agency2' AS source, given_name AS applicant_first_name, known_as AS applicant_nick_name,
       address AS applicant_home, 'vogue' AS firm_legal_name
FROM applications_b
```

| Column               | Type    | Key | Nullable | Property ID             |
|----------------------|---------|-----|----------|-------------------------|
| id                   | INT     | PK  | N        | id                      |
| source               | VARCHAR |     | Y        | source                  |
| applicant_first_name | VARCHAR |     | Y        | applicant.firstName     |
| applicant_nick_name  | VARCHAR |     | Y        | applicant.nickName      |
| applicant_home       | VARCHAR |     | Y        | applicant.address.name  |
| firm_legal_name      | VARCHAR |     | Y        | firm.legalName          |
