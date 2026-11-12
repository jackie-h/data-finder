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
