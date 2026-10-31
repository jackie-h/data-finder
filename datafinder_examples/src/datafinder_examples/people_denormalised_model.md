# People Denormalised Model

## Sub-Domain: people

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| PersonId  | personId  | Integer | 1            | Y   |             |
| FirstName | firstName | String  | 1            |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| LegalName | legalName | String | 1            |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 0..1         |     |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Firm                 | firm               | *                   | Firm   | Employees            | employees          | 1..*                |             |

### Association: PersonAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person | Address              | address            | *                   | Address | Residents            | residents          | *                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Address              | address            | *                   | Address | Firms                | firms              | 0..1                |             |
