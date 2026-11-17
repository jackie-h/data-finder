# Table Milestoning Model

## Sub-Domain: quotes.market

### Class: Quote

| Name  | Description                         |
|-------|-------------------------------------|
| Quote | A quote with a business range       |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |

### Class: Rate

| Name | Description                       |
|------|-----------------------------------|
| Rate | A rate with a business date       |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Code     | code | String | 1            | Y   |             |
| Rate     | rate | Double | 1            |     |             |

### Class: Fee

| Name | Description                                                          |
|------|----------------------------------------------------------------------|
| Fee  | A fee, whose table keeps its processing columns under other names    |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |
