# Diamond Inheritance Mapping

## Model: ../../model_markdown/tests/diamond_model.md

## DataStore: diamond_db (Database)

### Schema: records

#### Table: items → Record

| Column      | Type    | Key | Property    |
|-------------|---------|-----|-------------|
| item_id     | INT     | PK  | id          |
| created_at  | VARCHAR |     | created_at  |
| updated_at  | VARCHAR |     | updated_at  |
| version     | INT     |     | version     |
| record_name | VARCHAR |     | record_name |
