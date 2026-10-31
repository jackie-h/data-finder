# Legend Milestoning Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the bitemporal BiTemporalProduct and BiTemporalProductClassification subset).

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
