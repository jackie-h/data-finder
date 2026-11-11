# Legend Bitemporal Types On Unmilestoned Tables Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (Order, BiTemporalProduct and
BiTemporalProductClassification). In Legend these two types are bitemporal; Order is not milestoned. The mapping in
`bitemporal_none_mapping.md` (`noMilestoningMap`) maps the bitemporal types to tables that have no milestoning columns, so
the dates a query gives are ignored.

## Sub-Domain: milestoning

### Class: Order

| Name  | Description |
|-------|-------------|
| Order |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Id       | id | Integer | 1            | Y   |             |

### Class: BiTemporalProduct

| Name              | Description |
|-------------------|-------------|
| BiTemporalProduct |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |
| Type     | type | String  | 1            |     |             |

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

### Association: OrderBiTemporalProduct

| Name                   | Source | Source Property Name | Source Property ID | Source Multiplicity | Target            | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------------|--------|----------------------|--------------------|---------------------|-------------------|----------------------|--------------------|---------------------|-------------|
| OrderBiTemporalProduct | Order  | BiTemporalProduct    | biTemporalProduct  | *                   | BiTemporalProduct |                      |                    | *                   |             |
