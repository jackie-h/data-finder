# Legend Union Application Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (Application, with its Person and Firm that have a nick name),
as `tests/mapping/union/testUnion.pure` uses them. The properties of Application and Person that a mapping gives a constant are left out.

## Sub-Domain: application

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| NickName | nickName | String  | 0..1         |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| NickName | nickName | String  | 0..1         |     |             |

### Class: Application

| Name        | Description |
|-------------|-------------|
| Application |             |

| Property | Id   | Type | Multiplicity | Key | Description |
|----------|------|------|--------------|-----|-------------|
| Date     | date | Date | 1            |     |             |

### Association: ApplicationApplicant

| Name                 | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| ApplicationApplicant | Application | Applicant            | applicant          | 1                   | Person | Applications         | applications       | *                   |             |

### Association: ApplicationFirm

| Name            | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| ApplicationFirm | Application | Firm                 | firm               | 1                   | Firm   | Applications         | applications       | *                   |             |
