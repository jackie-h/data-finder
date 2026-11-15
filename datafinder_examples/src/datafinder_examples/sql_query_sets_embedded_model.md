# SQL Query Sets Embedded Model

## Sub-Domain: org.applications

### Class: Application

| Name        | Description                                                         |
|-------------|---------------------------------------------------------------------|
| Application | An application, with its applicant and the firm that is applied to  |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Id       | id     | Integer | 1            | Y   |             |
| Source   | source | String  | 1            |     |             |

### Class: Person

| Name   | Description |
|--------|-------------|
| Person | A person    |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| FirstName | firstName | String | 1            |     |             |
| NickName  | nickName  | String | 0..1         |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm | A firm      |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| LegalName | legalName | String | 1            |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address | An address  |

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
