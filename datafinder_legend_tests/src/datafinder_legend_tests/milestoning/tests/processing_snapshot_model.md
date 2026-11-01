# Legend Processing Snapshot Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the Order, Product and
ProductClassification subset the processing snapshot tests use). In Legend, ProcessingTemporalProduct and ProcessingTemporalProductClassification are
processing temporal and Order is not. Order has a `processingTemporalProduct` property and ProcessingTemporalProduct a `classification` property, and
neither has an accessor back, so each is a one-way association.

## Sub-Domain: milestoning

### Class: Order

| Name  | Description |
|-------|-------------|
| Order |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Id       | id | Integer | 1            | Y   |             |

### Class: ProcessingTemporalProduct

| Name                      | Description |
|---------------------------|-------------|
| ProcessingTemporalProduct |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: ProcessingTemporalProductClassification

| Name                                  | Description |
|---------------------------------------|-------------|
| ProcessingTemporalProductClassification |             |

| Property    | Id          | Type   | Multiplicity | Key | Description |
|-------------|-------------|--------|--------------|-----|-------------|
| Type        | type        | String | 1            | Y   |             |
| Description | description | String | 1            |     |             |

### Association: OrderProcessingTemporalProduct

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| OrderProcessingTemporalProduct | Order  | ProcessingTemporalProduct | processingTemporalProduct | 1 | ProcessingTemporalProduct |                      |                    | *                   |             |

### Association: ProductClassificationLink

| Name                      | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target                | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------------|---------|----------------------|--------------------|---------------------|-----------------------|----------------------|--------------------|---------------------|-------------|
| ProductClassificationLink | ProcessingTemporalProduct | Classification       | classification     | 1                   | ProcessingTemporalProductClassification |                      |                    | *                   |             |
