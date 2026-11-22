# Joins Through Tables Model

## Sub-Domain: people.hr

### Class: Employee

| Name     | Description                                                                 |
|----------|-----------------------------------------------------------------------------|
| Employee | A person on the payroll, whose home address is held in the employee's row   |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: Address

| Name    | Description          |
|---------|----------------------|
| Address | Where someone lives  |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Street   | street | String | 1            | Y   |             |

### Class: Country

| Name    | Description |
|---------|-------------|
| Country | A country   |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Code     | code | String | 1            | Y   |             |
| Name     | name | String | 1            |     |             |

### Association: EmployeeHome

| Name         | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|----------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| EmployeeHome | Employee | Home                 | home               | 1                   | Address |                      |                    | *                   |             |

### Association: AddressCountry

| Name           | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|---------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| AddressCountry | Address | Country              | country            | 1                   | Country |                      |                    | *                   |             |

### Association: EmployeeCitizenship

| Name                | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------|----------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| EmployeeCitizenship | Employee | Citizenship          | citizenship        | 0..1                | Country |                      |                    | *                   |             |
