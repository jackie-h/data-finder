# Legend Embedded Test Model

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedMapping.pure` (the Product, BondDetail and Party
classes, without enumerations, qualified properties or the Person, Firm and Org classes).

## Sub-Domain: embedded

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id     | Type   | Key | Description |
|----------|--------|--------|-----|-------------|
| Name     | name   | String |     |             |
| Market   | market | String |     |             |

### Class: BondDetail

| Name       | Description |
|------------|-------------|
| BondDetail |             |

| Property    | Id          | Type   | Key | Description |
|-------------|-------------|--------|-----|-------------|
| BondId      | bondId      | Integer | Y   |             |
| Description | description | String |     |             |
| Type        | type        | String |     |             |

### Class: Party

| Name  | Description |
|-------|-------------|
| Party |             |

| Property | Id   | Type   | Key | Description |
|----------|------|--------|-----|-------------|
| PartyId  | partyId | Integer | Y |           |
| Name     | name | String |     |             |

### Association: ProductBondDetails

| Name               | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target     | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------------|---------|----------------------|--------------------|---------------------|------------|----------------------|--------------------|---------------------|-------------|
| ProductBondDetails | Product | Products             | products           | *                   | BondDetail | BondDetails          | bondDetails        | 1                   |             |

### Association: BondDetailHolder

| Name             | Source     | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------|------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| BondDetailHolder | BondDetail | BondDetails          | bondDetails        | *                   | Party  | Holder               | holder             | 1                   |             |
