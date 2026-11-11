# Legend Chained Joins Model

Source: legend-engine core_relational `tests/mapping/join/advancedRelationalTestModel.pure` (Person and Firm). A firm's
employees are reached through a bridge table of firm and person keys.

## Sub-Domain: join

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| FirstName | firstName | String  | 1            |     |             |
| LastName  | lastName  | String  | 1            |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| LegalName | legalName | String  | 1            |     |             |

### Association: FirmEmployees

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| FirmEmployees | Firm   | Employees            | employees          | *                   | Person |                      |                    | *                   |             |
