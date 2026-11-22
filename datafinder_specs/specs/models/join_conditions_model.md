# Join Conditions Model

## Sub-Domain: org.staff

### Class: Firm

| Name | Description |
|------|-------------|
| Firm | A firm      |

| Property  | Id         | Type    | Multiplicity | Key | Description |
|-----------|------------|---------|--------------|-----|-------------|
| Id        | id         | Integer | 1            | Y   |             |
| LegalName | legalName  | String  | 1            |     |             |

### Class: Person

| Name   | Description |
|--------|-------------|
| Person | A person    |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| FirstName | firstName | String  | 1            |     |             |

### Class: Employee

| Name     | Description                      |
|----------|----------------------------------|
| Employee | An employee, who can have a manager |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Association: EmployeeManager

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|----------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| EmployeeManager | Employee | Manager              | manager            | 0..1                | Employee | Reports              | reports            | *                   |             |

### Association: FirmPerson

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| FirmPerson | Person | Firm                 | firm               | 0..1                | Firm   | Employees            | employees          | *                   |             |

### Association: FirmNamedPerson

| Name            | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| FirmNamedPerson | Person | FirmIfPeterOrJohn    | firmIfPeterOrJohn  | 0..1                | Firm   |                      |                    | *                   |             |

### Association: PersonMemberFirm

| Name             | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonMemberFirm | Person | ActiveFirms          | activeFirms        | *                   | Firm   |                      |                    | *                   |             |
