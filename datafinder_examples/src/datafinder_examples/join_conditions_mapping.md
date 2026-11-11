# Join Conditions Mapping

A join can have a condition besides its key columns, as Legend's `Join Firm_Person(firms.id = persons.firm_id and ...)`.
An Association table with a `Condition` column gives each row's condition over the two tables the row joins, written as
`table.column`, so it reads the same from either end of the join. A person has a firm only when the firm's `flag` is NULL
and the person's `flag` is not.

## Model: join_conditions_model.md

## DataStore: org_db (Database)

### Schema: org

#### Table: firms → Firm

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| id         | INT     | PK  | N        | id          |
| legal_name | VARCHAR |     | Y        | legalName   |
| flag       | INT     |     | Y        |             |

#### Table: staff → Employee

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| id         | INT     | PK  | N        | id          |
| name       | VARCHAR |     | Y        | name        |
| manager_id | INT     | FK  | Y        |             |
| active     | INT     |     | Y        |             |

An employee is joined to itself, so `staff` is both sides of the condition: its own name is the employee the join starts from and
`{target}` the one it reaches, as in Legend's `personTable.MANAGERID = {target}.ID`. Both have to be active.

#### Association: EmployeeManager

| Source Column | Target Table | Target Column | Condition                                  |
|---------------|--------------|---------------|--------------------------------------------|
| manager_id    | staff        | id            | staff.active = 1 AND {target}.active = 1 |

#### Table: persons → Person

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| id         | INT     | PK  | N        | id          |
| first_name | VARCHAR |     | Y        | firstName   |
| flag       | INT     |     | Y        |             |
| firm_id    | INT     | FK  | Y        |             |

#### Association: FirmPerson

| Source Column | Target Table | Target Column | Condition                                         |
|---------------|--------------|---------------|---------------------------------------------------|
| firm_id       | firms        | id            | firms.flag IS NULL AND persons.flag IS NOT NULL |

#### Association: FirmNamedPerson

| Source Column | Target Table | Target Column | Condition                                                      |
|---------------|--------------|---------------|----------------------------------------------------------------|
| firm_id       | firms        | id            | (persons.first_name = 'Peter' OR persons.first_name = 'John') |

#### Association: PersonMemberFirm

| Source Column | Target Table | Target Column | Condition              |
|---------------|--------------|---------------|------------------------|
| id            | memberships  | person_id     |                        |
| firm_id       | firms        | id            | memberships.active = 1 |

#### Table: memberships

| Column    | Type | Key | Nullable | Property ID |
|-----------|------|-----|----------|-------------|
| person_id | INT  |     | Y        |             |
| firm_id   | INT  |     | Y        |             |
| active    | INT  |     | Y        |             |
