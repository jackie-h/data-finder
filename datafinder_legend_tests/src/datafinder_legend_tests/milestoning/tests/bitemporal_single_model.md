# Legend Single Temporal Store Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (BiTemporalProduct and
BiTemporalProductClassification). In Legend these two types are bitemporal; the mapping in `bitemporal_single_mapping.md`
(`singleTemporalMappingForBiTemporalTypes`) maps them to tables that have a business range only.

## Sub-Domain: milestoning

### Class: BiTemporalProduct

| Name              | Description |
|-------------------|-------------|
| BiTemporalProduct |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: BiTemporalProductClassification

| Name                            | Description |
|---------------------------------|-------------|
| BiTemporalProductClassification |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Type     | type | String | 1            | Y   |             |

### Association: BiTemporalProductClassificationLink

| Name                                | Source            | Source Property Name     | Source Property ID       | Source Multiplicity | Target                          | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------------------------|-------------------|--------------------------|--------------------------|---------------------|---------------------------------|----------------------|--------------------|---------------------|-------------|
| BiTemporalProductClassificationLink | BiTemporalProduct | BiTemporalClassification | biTemporalClassification | 0..1                | BiTemporalProductClassification |                      |                    | *                   |             |
