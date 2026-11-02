# Joins Through Tables Mapping

An employee's home address is held in the employee's row: its street is the `street` column, and its country is reached
from the `home_country` column. A country is not named in the employee's row but by a number that `country_refs` turns
into a code, and `countries` has the name, so reaching a country goes through `country_refs`, whose rows are valid for a
range of dates: an Association table with
more than one row is a path through the tables it names, each row starting from a column of the table the row before
it reached. An Association for a property of an embedded object says where it is embedded.

## Model: joins_through_tables_model.md

## DataStore: hr_db (Database)

| Scheme        | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| link_validity |                  |                |               | valid_from         | valid_to         | 9999-12-31        |

### Schema: hr

#### Table: employees → Employee

| Column       | Type    | Key | Nullable | Property ID  |
|--------------|---------|-----|----------|--------------|
| id           | INT     | PK  | N        | id           |
| name         | VARCHAR |     | Y        | name         |
| street       | VARCHAR |     | Y        | home.street  |
| home_country | INT     | FK  | Y        | home.country |
| citizen_ref  | INT     | FK  | Y        | citizenship  |

#### Table: country_refs (milestoning: link_validity)

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| ref        | INT     | PK  | N        |             |
| code       | VARCHAR |     | Y        |             |
| valid_from | DATE    |     | Y        |             |
| valid_to   | DATE    |     | Y        |             |

#### Table: countries → Country

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| code   | VARCHAR | PK  | N        | code        |
| name   | VARCHAR |     | Y        | name        |

#### Association: AddressCountry (embedded in Employee.home)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| home_country  | country_refs | ref           |
| code          | countries    | code          |

#### Association: EmployeeCitizenship

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| citizen_ref   | country_refs | ref           |
| code          | countries    | code          |
