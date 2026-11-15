# Legend Union Application Embedded Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (Application, with its Person, Firm and Address), as `tests/mapping/union/testUnion.pure` uses them.
The properties of Person and Firm that a mapping leaves out are left out.

## Sub-Domain: application

### Class: Application

| Name        | Description |
|-------------|-------------|
| Application |             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Date     | date   | Date   | 1            | Y   |             |
| Source   | source | String | 1            |     |             |

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| FirstName | firstName | String | 0..1         |     |             |
| LastName  | lastName  | String | 0..1         |     |             |
| NickName  | nickName  | String | 0..1         |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| LegalName | legalName | String | 0..1         |     |             |
| NickName  | nickName  | String | 0..1         |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 0..1         |     |             |

### Association: ApplicationApplicant

| Name                 | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| ApplicationApplicant | Application | Applicant            | applicant          | 1                   | Person | Applications         | applications       | *                   |             |

### Association: ApplicationFirm

| Name            | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| ApplicationFirm | Application | Firm                 | firm               | 1                   | Firm   | Applications         | applications       | *                   |             |

### Association: PersonAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person | Address              | address            | 0..1                | Address | Residents            | residents          | *                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Address              | address            | 0..1                | Address | Firms                | firms              | *                   |             |
