# Legend Partial Union Mapping: The Address Joined On Its Key

Source: legend-engine core_relational `tests/mapping/union/testUnionPartial.pure` (`partialUnionMappingOfAddressWithManyPropertyMappings_AddressIdKey`).
Person is the union of two sets and Address of two sets, but only the first set of Person has an address: its address is a row of either set of Address, joined on `ADDRESSID` of the person.
The second set of Person has no address, and the employees of a Firm are the people of either set.

## Model: ../../simple_model.md

## DataStore: myDB (Database)

### Schema: main

#### Table: PersonSet1

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| ID          | INT     | PK  | N        |             |
| lastName_s1 | VARCHAR |     | Y        |             |
| FirmID      | INT     |     | Y        |             |
| ADDRESSID   | INT     |     | Y        |             |

#### Table: PersonSet2

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| ID          | INT     | PK  | N        |             |
| lastName_s2 | VARCHAR |     | Y        |             |
| FirmID      | INT     |     | Y        |             |
| ADDRESSID   | INT     |     | Y        |             |

#### Table: AddressSet1

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

#### Table: AddressSet2

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

#### Table: Firm → Firm

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        | id          |
| name   | VARCHAR |     | Y        | legalName   |

#### Query: PERSON_SET1 → Person (set: set1)

```sql
SELECT ID, lastName_s1 AS lastName, FirmID, ADDRESSID FROM PersonSet1
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| lastName  | VARCHAR |     | Y        | lastName    |
| FirmID    | INT     |     | Y        |             |
| ADDRESSID | INT     |     | Y        |             |

#### Query: PERSON_SET2 → Person (set: set2)

```sql
SELECT ID, lastName_s2 AS lastName, FirmID, ADDRESSID FROM PersonSet2
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| lastName  | VARCHAR |     | Y        | lastName    |
| FirmID    | INT     |     | Y        |             |
| ADDRESSID | INT     |     | Y        |             |

#### Query: ADDRESS_SET1 → Address (set: add1)

```sql
SELECT id, name FROM AddressSet1
```

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        | name        |

#### Query: ADDRESS_SET2 → Address (set: add2)

```sql
SELECT id, name FROM AddressSet2
```

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        | name        |

#### Association: PersonFirm (source set: set1)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FirmID        | Firm         | id            |

#### Association: PersonFirm (source set: set2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FirmID        | Firm         | id            |

#### Association: PersonAddress (source set: set1, target set: add1)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID | ADDRESS_SET1 | id            |

#### Association: PersonAddress (source set: set1, target set: add2)

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID | ADDRESS_SET2 | id            |
