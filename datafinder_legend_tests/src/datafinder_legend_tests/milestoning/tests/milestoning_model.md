# Legend Milestoning Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the bitemporal BiTemporalProduct and BiTemporalProductClassification subset).

## Sub-Domain: milestoning

### Class: BiTemporalProduct

| Name              | Description |
|-------------------|-------------|
| BiTemporalProduct |             |

| Property | Id   | Type    | Key | Description |
|----------|------|---------|-----|-------------|
| Id       | id   | Integer | Y   |             |
| Type     | type | String  |     |             |

### Class: BiTemporalProductClassification

| Name                          | Description |
|-------------------------------|-------------|
| BiTemporalProductClassification |           |

| Property | Id   | Type   | Key | Description |
|----------|------|--------|-----|-------------|
| Type     | type | String | Y   |             |
| System   | system | String |   |             |
| Exchange | exchange | String |  |            |

### Association: BiTemporalProductClassificationLink

| Name                              | Source            | Source Property Name | Source Property ID | Source Multiplicity | Target                          | Target Property Name      | Target Property ID        | Target Multiplicity | Description |
|-----------------------------------|-------------------|----------------------|--------------------|---------------------|---------------------------------|---------------------------|---------------------------|---------------------|-------------|
| BiTemporalProductClassificationLink | BiTemporalProduct | Products             | products           | *                   | BiTemporalProductClassification | BiTemporalClassification  | biTemporalClassification  | 1                   |             |
