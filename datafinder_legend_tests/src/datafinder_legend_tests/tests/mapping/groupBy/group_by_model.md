# Legend Group By Model

Source: legend-engine core_relational `tests/mapping/groupBy/testGroupBy.pure` (Position, Product and the association between them).

## Sub-Domain: groupby

### Class: Position

| Name     | Description |
|----------|-------------|
| Position |             |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| AcctNum  | acctNum  | Integer | 1            | Y   |             |
| Gsn      | gsn      | String  | 1            | Y   |             |
| Quantity | quantity | Integer | 1            |     |             |

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property    | Id          | Type    | Multiplicity | Key | Description |
|-------------|-------------|---------|--------------|-----|-------------|
| Id          | id          | Integer | 1            | Y   |             |
| Description | description | String  | 1            |     |             |

### Association: PositionProduct

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|----------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PositionProduct | Position | Product              | product            | 0..1                | Product | Positions            | positions          | *                   |             |
