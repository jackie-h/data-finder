# SQL Queries Model

## Sub-Domain: org.views

### Class: Firm

| Name | Description                                                       |
|------|-------------------------------------------------------------------|
| Firm | A firm, which is the distinct firms of the table of its people    |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| LegalName | legalName | String  | 1            |     |             |

### Class: Person

| Name   | Description |
|--------|-------------|
| Person | A person    |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| FirstName | firstName | String  | 1            |     |             |
| Age       | age       | Integer | 1            |     |             |

### Class: Position

| Name     | Description                                  |
|----------|----------------------------------------------|
| Position | Trades added up by account, product and gsn  |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| Gsn      | gsn      | String  | 1            | Y   |             |
| Quantity | quantity | Integer | 1            |     |             |

### Class: Tag

| Name | Description                                      |
|------|--------------------------------------------------|
| Tag  | A person's name made into text by an expression |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Text     | text | String  | 1            |     |             |

### Class: FirmXPerson

| Name        | Description                              |
|-------------|------------------------------------------|
| FirmXPerson | A person of the firm called Firm X only  |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| Id        | id        | Integer | 1           | Y   |             |
| FirstName | firstName | String  | 1           |     |             |

### Class: Shout

| Name  | Description                                       |
|-------|---------------------------------------------------|
| Shout | A firm's name in capitals, from the query of firms |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Text     | text | String  | 1            |     |             |

### Class: Rate

| Name | Description                                                                               |
|------|-------------------------------------------------------------------------------------------|
| Rate | A rate that applies over a range of business dates, from a query that drops the ones with no amount |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |

### Class: Quote

| Name  | Description                                                                              |
|-------|------------------------------------------------------------------------------------------|
| Quote | A quote with a processing and a business range, from a query that drops the ones with no amount |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Code     | code   | String | 1            | Y   |             |
| Amount   | amount | Double | 1            |     |             |

### Class: Holder

| Name   | Description                                                |
|--------|------------------------------------------------------------|
| Holder | Holds a rate, which is milestoned, by its code             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |

### Association: HolderRate

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| HolderRate | Holder | Rate                 | rate               | 0..1                | Rate   |                      |                    | *                   |             |

### Association: FirmPerson

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| FirmPerson | Firm   | Employees            | employees          | *                   | Person |                      |                    | *                   |             |
