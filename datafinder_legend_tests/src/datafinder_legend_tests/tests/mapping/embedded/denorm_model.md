# Legend Denormalised Test Model

Source: legend-engine core_relational `tests/mapping/embedded/testEmbeddedMapping.pure` (Person, Firm and Address, without the
enumerations, qualified properties and the Organization and Location classes).

## Sub-Domain: denorm

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Key | Description |
|-----------|-----------|---------|-----|-------------|
| PersonId  | personId  | Integer | Y   |             |
| FirstName | firstName | String  |     |             |
| LastName  | lastName  | String  |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type   | Key | Description |
|-----------|-----------|--------|-----|-------------|
| LegalName | legalName | String |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id   | Type   | Key | Description |
|----------|------|--------|-----|-------------|
| Name     | name | String |     |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Employees            | employees          | *                   | Firm   | Firm                 | firm               | 1                   |             |

### Association: PersonAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person | Residents            | residents          | *                   | Address | Address              | address            | 1                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Firms                | firms              | *                   | Address | Address              | address            | 1                   |             |
