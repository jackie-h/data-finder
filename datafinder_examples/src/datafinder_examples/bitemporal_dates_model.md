# Bitemporal Dates Model

## Sub-Domain: quotes.dated

### Class: Quote

| Name  | Description                                                                                       |
|-------|---------------------------------------------------------------------------------------------------|
| Quote | A quote whose processing range ends at a datetime infinity and whose business range ends at a date infinity |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |
