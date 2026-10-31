# Legend Embedded Test Model

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedMapping.pure` (the Product, BondDetail and Party
classes, without enumerations, qualified properties or the Person, Firm and Org classes).

## Sub-Domain: embedded

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Name     | name   | String | 1            |     |             |
| Market   | market | String | 1            |     |             |

### Class: BondDetail

| Name       | Description |
|------------|-------------|
| BondDetail |             |

| Property    | Id          | Type    | Multiplicity | Key | Description |
|-------------|-------------|---------|--------------|-----|-------------|
| BondId      | bondId      | Integer | 1            | Y   |             |
| Description | description | String  | 1            |     |             |
| Type        | type        | String  | 1            |     |             |

### Class: Party

| Name  | Description |
|-------|-------------|
| Party |             |

| Property | Id      | Type    | Multiplicity | Key | Description |
|----------|---------|---------|--------------|-----|-------------|
| PartyId  | partyId | Integer | 1            | Y   |             |
| Name     | name    | String  | 1            |     |             |

### Association: ProductBondDetails

| Name               | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target     | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------------|---------|----------------------|--------------------|---------------------|------------|----------------------|--------------------|---------------------|-------------|
| ProductBondDetails | Product | BondDetails          | bondDetails        | 1                   | BondDetail |                      |                    | *                   |             |

### Association: BondDetailHolder

| Name             | Source     | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------|------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| BondDetailHolder | BondDetail | Holder               | holder             | 1                   | Party  |                      |                    | *                   |             |
