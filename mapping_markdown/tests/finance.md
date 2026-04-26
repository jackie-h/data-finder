# Finance Model

## Sub-Domain: finance

### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Type    | Key | Description |
|----------|---------|-----|-------------|
| id       | Integer | Y   |             |
| name     | String  |     |             |

### Class: Instrument

| Name       | Description |
|------------|-------------|
| Instrument |             |

| Property   | Type     | Key | Description |
|------------|----------|-----|-------------|
| symbol     | String   | Y   |             |
| price      | Double   |     |             |
| valid_from | DateTime |     |             |
| valid_to   | DateTime |     |             |
