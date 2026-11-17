# Table Milestoning Model

## Sub-Domain: quotes.market

### Class: Quote

| Name  | Description                         |
|-------|-------------------------------------|
| Quote | A quote, milestoned as its scheme says |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |

### Class: Rate

| Name | Description                       |
|------|-----------------------------------|
| Rate | A rate, whose business range is in columns of its own |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Code     | code | String | 1            | Y   |             |
| Rate     | rate | Double | 1            |     |             |

### Class: Fee

| Name | Description                                                          |
|------|----------------------------------------------------------------------|
| Fee  | A fee, which has a business date in place of a business range |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |
