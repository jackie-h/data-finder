# Places Mapping

A person has locations, and a location is in a region. A navigation can be filtered, and the condition then belongs to its join: a person
with no location that holds is still there, with nothing for the location.

## Model: ../models/places_model.md

## DataStore: places_db (Database)

### Schema: geo

#### Table: people → Person

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        | id          |
| name   | VARCHAR |     | Y        | name        |

#### Table: regions → Region

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        | id          |
| name   | VARCHAR |     | Y        | name        |

#### Table: locations → Location

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| id        | INT     | PK  | N        | id          |
| place     | VARCHAR |     | Y        | place       |
| visits    | INT     |     | Y        | visits      |
| person_id | INT     | FK  | Y        | person      |
| region_id | INT     | FK  | Y        | region      |

#### Association: PersonLocation

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| person_id     | people       | id            |

#### Association: LocationRegion

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| region_id     | regions      | id            |
