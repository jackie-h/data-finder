# Legend Distinct Model

Source: legend-engine core_relational `tests/mapping/distinct/testDistinct.pure` (IncomeFunction and Classification).

## Sub-Domain: distinct

### Class: IncomeFunction

| Name           | Description |
|----------------|-------------|
| IncomeFunction |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Code     | code | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |
| IfName   | ifName | String | 0..1         |     |             |

### Class: Classification

| Name           | Description |
|----------------|-------------|
| Classification |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 1            | Y   |             |

### Association: ClassificationIncomeFunctions

| Name                        | Source         | Source Property Name | Source Property ID | Source Multiplicity | Target         | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------------------|----------------|----------------------|--------------------|---------------------|----------------|----------------------|--------------------|---------------------|-------------|
| ClassificationIncomeFunctions | Classification | IncomeFunctions      | incomeFunctions    | *                   | IncomeFunction | Classification       | classification     | 0..1                |             |

### Class: FirmAccount

| Name        | Description |
|-------------|-------------|
| FirmAccount |             |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Number   | number | Integer | 1            | Y   |             |
| Name     | name   | String  | 1            |     |             |

### Class: DeskHead

| Name     | Description |
|----------|-------------|
| DeskHead |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Name     | name | String  | 1            |     |             |
| Code     | code | Integer | 1            | Y   |             |

### Association: FirmAccountIncomeFunction

| Name                       | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target         | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------------------|-------------|----------------------|--------------------|---------------------|----------------|----------------------|--------------------|---------------------|-------------|
| FirmAccountIncomeFunction  | FirmAccount | IncomeFunction       | incomeFunction     | 0..1                | IncomeFunction | FirmAccounts         | firmAccounts       | *                   |             |

### Association: IncomeFunctionDeskHead

| Name                    | Source         | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------------|----------------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| IncomeFunctionDeskHead  | IncomeFunction | DeskHead             | deskHead           | 0..1                | DeskHead | IncomeFunctions      | incomeFunctions    | *                   |             |
