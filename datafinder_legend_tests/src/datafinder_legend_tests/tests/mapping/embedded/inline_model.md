# Legend Inline Embedded Test Model

Source: legend-engine core_relational `tests/mapping/embedded/testInlineEmbeddedMapping.pure` (Product, BondDetail, Party
and Address, without the enumerations, qualified properties, Person classes and the BondClassification association).
The classes have no key in Legend; `Id` is the table's key, which the finders need and the queries never use. Legend
writes each embedded class as its own class mapping that the owner inlines; here the embedded object's columns are
written on the table with dotted property ids, which reads the same columns.

## Sub-Domain: inline

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Id       | id     | Integer | 1            | Y   |             |
| Name     | name   | String  | 1            |     |             |
| Market   | market | String  | 1            |     |             |

### Class: BondDetail

| Name       | Description |
|------------|-------------|
| BondDetail |             |

| Property    | Id          | Type    | Multiplicity | Key | Description |
|-------------|-------------|---------|--------------|-----|-------------|
| Id          | id          | Integer | 1            | Y   |             |
| Description | description | String  | 1            |     |             |
| Type        | type        | String  | 1            |     |             |

### Class: Party

| Name  | Description |
|-------|-------------|
| Party |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 1            |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id     | Type   | Multiplicity | Key | Description |
|----------|--------|--------|--------------|-----|-------------|
| Name     | name   | String | 1            |     |             |
| Street   | street | String | 1            |     |             |

### Association: ProductBondDetails

| Name                | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target     | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------|---------|----------------------|--------------------|---------------------|------------|----------------------|--------------------|---------------------|-------------|
| ProductBondDetails  | Product | Bond Details         | bondDetails        | 1                   | BondDetail |                      |                    | *                   |             |

### Association: BondDetailIssuer

| Name              | Source     | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------|------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| BondDetailIssuer  | BondDetail | Issuer               | issuer             | 1                   | Party  |                      |                    | *                   |             |

### Association: BondDetailHolder

| Name              | Source     | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------|------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| BondDetailHolder  | BondDetail | Holder               | holder             | 1                   | Party  |                      |                    | *                   |             |

### Association: PartyAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PartyAddress  | Party  | Address              | address            | 1                   | Address |                      |                    | *                   |             |
