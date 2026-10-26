# People Denormalised Mapping

A person's row also holds their address, their firm and the firm's address. There is no table behind them, so the
embedded properties are read straight from the row.

## Model: people_denormalised_model.md

## DataStore: people_db (Database)

### Schema: main

#### Table: people → Person

| Column       | Type    | Key | Nullable | Property ID       |
|--------------|---------|-----|----------|-------------------|
| id           | INT     | PK  | N        | personId          |
| first_name   | VARCHAR |     | Y        | firstName         |
| home         | VARCHAR |     | Y        | address.name      |
| firm_name    | VARCHAR |     | Y        | firm.legalName    |
| firm_address | VARCHAR |     | Y        | firm.address.name |
