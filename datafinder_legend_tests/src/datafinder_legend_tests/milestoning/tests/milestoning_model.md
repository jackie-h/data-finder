# Legend Milestoning Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the BiTemporalProduct and BiTemporalProductClassification, with the Order, Product, ProductClassification, Trader and
Location that reach them). In Legend, Order is not milestoned, Product and ProductClassification are business temporal,
Trader is processing temporal, and the others are bitemporal.

## Sub-Domain: milestoning

### Class: BiTemporalProduct

| Name              | Description |
|-------------------|-------------|
| BiTemporalProduct |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Type     | type | String  | 1            |     |             |

### Class: BiTemporalProductClassification

| Name                            | Description |
|---------------------------------|-------------|
| BiTemporalProductClassification |             |

| Property | Id       | Type   | Multiplicity | Key | Description |
|----------|----------|--------|--------------|-----|-------------|
| Type     | type     | String | 1            | Y   |             |
| System   | system   | String | 1            |     |             |
| Exchange | exchange | String | 1            |     |             |

### Association: BiTemporalProductClassificationLink

| Name                                | Source            | Source Property Name     | Source Property ID       | Source Multiplicity | Target                          | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------------------------|-------------------|--------------------------|--------------------------|---------------------|---------------------------------|----------------------|--------------------|---------------------|-------------|
| BiTemporalProductClassificationLink | BiTemporalProduct | BiTemporalClassification | biTemporalClassification | 0..1                | BiTemporalProductClassification |                      |                    | *                   |             |

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

### Class: Trader

| Name   | Description |
|--------|-------------|
| Trader |             |

| Property | Id       | Type   | Multiplicity | Key | Description |
|----------|----------|--------|--------------|-----|-------------|
| Kerberos | kerberos | String | 1            | Y   |             |

### Class: Location

| Name     | Description |
|----------|-------------|
| Location |             |

| Property | Id    | Type   | Multiplicity | Key | Description |
|----------|-------|--------|--------------|-----|-------------|
| Place    | place | String | 1            | Y   |             |

### Association: OrderBiTemporalProduct

| Name                   | Source | Source Property Name | Source Property ID | Source Multiplicity | Target            | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------------|--------|----------------------|--------------------|---------------------|-------------------|----------------------|--------------------|---------------------|-------------|
| OrderBiTemporalProduct | Order  | BiTemporalProduct    | biTemporalProduct  | *                   | BiTemporalProduct |                      |                    | *                   |             |

### Association: BiTemporalProductClassificationSingle

| Name                                | Source            | Source Property Name | Source Property ID | Source Multiplicity | Target                | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------------------------|-------------------|----------------------|--------------------|---------------------|-----------------------|----------------------|--------------------|---------------------|-------------|
| BiTemporalProductClassificationSingle | BiTemporalProduct | Classification       | classification     | 1                   | ProductClassification |                      |                    | *                   |             |

### Association: ProductBiTemporalClassification

| Name                            | Source  | Source Property Name     | Source Property ID       | Source Multiplicity | Target                          | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------------------|---------|--------------------------|--------------------------|---------------------|---------------------------------|----------------------|--------------------|---------------------|-------------|
| ProductBiTemporalClassification | Product | BiTemporalClassification | biTemporalClassification | 0..1                | BiTemporalProductClassification |                      |                    | *                   |             |

### Association: TraderLocation

| Name           | Source | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|--------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| TraderLocation | Trader | Location             | location           | 0..1                | Location |                      |                    | *                   |             |
