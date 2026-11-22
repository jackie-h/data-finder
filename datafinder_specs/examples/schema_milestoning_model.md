# Schema Milestoning Model

## Sub-Domain: quotes.market

### Class: Quote

| Name  | Description                                   |
|-------|-----------------------------------------------|
| Quote | A quote in the market data schema             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |

### Class: Rate

| Name | Description                                    |
|------|------------------------------------------------|
| Rate | A rate in the reference data schema            |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Code     | code | String | 1            | Y   |             |
| Rate     | rate | Double | 1            |     |             |
