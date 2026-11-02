# Legend Embedded System Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the Product, ProductClassification and
System subset the embedded milestoning tests use). A Product holds its classification in its own row (an embedded
object), and the classification's system is another table. In Legend Product is business temporal and System is not.

## Sub-Domain: milestoning

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: ProductClassification

| Name                  | Description |
|-----------------------|-------------|
| ProductClassification |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Type     | type | String | 1            | Y   |             |

### Class: System

| Name   | Description |
|--------|-------------|
| System |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 1            | Y   |             |

### Association: ProductClassificationLink

| Name                      | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target                | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------------|---------|----------------------|--------------------|---------------------|-----------------------|----------------------|--------------------|---------------------|-------------|
| ProductClassificationLink | Product | Classification       | classification     | 1                   | ProductClassification |                      |                    | *                   |             |

### Association: ProductClassificationSystem

| Name                       | Source                | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------------------|-----------------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| ProductClassificationSystem | ProductClassification | System               | system             | 1                   | System |                      |                    | *                   |             |
