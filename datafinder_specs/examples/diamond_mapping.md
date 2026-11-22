# Diamond Inheritance Mapping

## Model: diamond_model.md

## DataStore: diamond_db (Database)

### Schema: records

#### Table: items → Record

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| item_id     | INT     | PK  | N        | id          |
| created_at  | VARCHAR |     | Y        | createdAt   |
| updated_at  | VARCHAR |     | Y        | updatedAt   |
| version     | INT     |     | Y        | version     |
| record_name | VARCHAR |     | Y        | recordName  |
