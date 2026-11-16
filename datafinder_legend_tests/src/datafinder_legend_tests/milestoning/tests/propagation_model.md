# Legend Milestoning Propagation Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the Product and NewActivityInfo subset the propagation tests use).
Product is business temporal in Legend and NewActivityInfo is not.

## Sub-Domain: milestoning

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Id       | id | Integer | 1            | Y   |             |

### Class: NewActivityInfo

| Name            | Description |
|-----------------|-------------|
| NewActivityInfo |             |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| CreatedBy | createdBy | String | 0..1         |     |             |

### Association: ProductNewActivity

| Name               | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target          | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------------|---------|----------------------|--------------------|---------------------|-----------------|----------------------|--------------------|---------------------|-------------|
| ProductNewActivity | Product | NewActivity          | newActivity        | 0..1                | NewActivityInfo |                      |                    | *                   |             |

### Association: ProductCancelActivity

| Name                  | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target          | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------------|---------|----------------------|--------------------|---------------------|-----------------|----------------------|--------------------|---------------------|-------------|
| ProductCancelActivity | Product | CancelActivity       | cancelActivity     | 0..1                | NewActivityInfo |                      |                    | *                   |             |
