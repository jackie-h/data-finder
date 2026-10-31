# Legend Business Snapshot Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the Order, Product and
ProductClassification subset the business snapshot tests use). In Legend, Product and ProductClassification are
business temporal and Order is not. Order has a `product` property and Product a `classification` property, and
neither has an accessor back, so each is a one-way association.

## Sub-Domain: milestoning

### Class: Order

| Name  | Description |
|-------|-------------|
| Order |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Id       | id | Integer | 1            | Y   |             |

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

| Property    | Id          | Type   | Multiplicity | Key | Description |
|-------------|-------------|--------|--------------|-----|-------------|
| Type        | type        | String | 1            | Y   |             |
| Description | description | String | 1            |     |             |

### Association: OrderProduct

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| OrderProduct | Order  | Product              | product            | *                   | Product |                      |                    | *                   |             |

### Association: ProductClassificationLink

| Name                      | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target                | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------------|---------|----------------------|--------------------|---------------------|-----------------------|----------------------|--------------------|---------------------|-------------|
| ProductClassificationLink | Product | Classification       | classification     | 1                   | ProductClassification |                      |                    | *                   |             |
