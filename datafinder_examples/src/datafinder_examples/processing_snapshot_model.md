# Processing Snapshot Model

## Sub-Domain: snapshots.market

### Class: Quote

| Name  | Description                                                             |
|-------|-------------------------------------------------------------------------|
| Quote | A price taken at a processing time, one row for each time it was taken  |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Id       | id     | Integer| 1            | Y   |             |
| Symbol   | symbol | String | 1            |     |             |
| Price    | price  | Double | 1            |     |             |

### Class: Position

| Name     | Description                       |
|----------|-----------------------------------|
| Position | A holding, which has no milestoning |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Id       | id     | Integer | 1            | Y   |             |

### Association: PositionQuotes

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PositionQuotes | Position | Quotes               | quotes             | *                   | Quote  |                      |                    | *                   |             |
