# Legend Simple Test Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (the Person, Firm, Address, Location subset).

## Sub-Domain: simple

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Key | Description |
|-----------|-----------|---------|-----|-------------|
| Id        | id        | Integer | Y   |             |
| FirstName | firstName | String  |     |             |
| LastName  | lastName  | String  |     |             |
| Age       | age       | Integer |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type    | Key | Description |
|-----------|-----------|---------|-----|-------------|
| Id        | id        | Integer | Y   |             |
| LegalName | legalName | String  |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id     | Type    | Key | Description |
|----------|--------|---------|-----|-------------|
| Id       | id     | Integer | Y   |             |
| Name     | name   | String  |     |             |
| Street   | street | String  |     |             |
| Comments | comments | String |    |             |

### Class: Location

| Name     | Description |
|----------|-------------|
| Location |             |

| Property | Id    | Type    | Key | Description |
|----------|-------|---------|-----|-------------|
| Id       | id    | Integer | Y   |             |
| Place    | place | String  |     |             |
| Censusdate | censusdate | Date |  |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Employees            | employees          | *                   | Firm   | Firm                 | firm               | 1                   |             |

### Association: PersonAddress

| Name          | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|---------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person  | Persons              | persons            | *                   | Address | Address             | address            | 1                   |             |

### Association: PersonManager

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonManager | Person | Reports              | reports            | *                   | Person | Manager              | manager            | 1                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Firms                | firms              | *                   | Address | Address              | address            | 1                   |             |

### Association: PersonLocations

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonLocations | Location | Locations            | locations          | *                   | Person | Person               | person             | 1                   |             |
