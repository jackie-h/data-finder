# Processing Snapshot Mapping

## Model: processing_snapshot_model.md

## DataStore: snapshots_db (Database)

| Scheme              | processing_start | processing_end | processing_datetime | business_date | business_date_from | business_date_to |
|---------------------|------------------|----------------|---------------------|---------------|--------------------|------------------|
| processing_snapshot |                  |                | taken_at            |               |                    |                  |

### Schema: mkt

#### Table: quotes → Quote (milestoning: processing_snapshot)

| Column   | Type      | Key | Nullable | Property ID |
|----------|-----------|-----|----------|-------------|
| id       | INT       | PK  | N        | id          |
| symbol   | VARCHAR   |     | Y        | symbol      |
| price    | DOUBLE    |     | Y        | price       |
| taken_at | TIMESTAMP |     | Y        | takenAt     |

#### Table: positions → Position

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        | id          |
| symbol | VARCHAR | FK  | Y        | quotes      |

#### Association: PositionQuotes

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| symbol        | quotes       | symbol        |
