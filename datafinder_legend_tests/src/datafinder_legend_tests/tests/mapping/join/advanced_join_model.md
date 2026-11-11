# Legend Advanced Join Model

Source: legend-engine core_relational `tests/mapping/join/advancedRelationalTestModel.pure` (Person and Firm). A person has a
firm, and a firm its employees, which is the same association followed from the other end.

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

### Association: FirmPerson

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| FirmPerson | Person | Firm                 | firm               | 0..1                | Firm   | Employees            | employees          | *                   |             |
