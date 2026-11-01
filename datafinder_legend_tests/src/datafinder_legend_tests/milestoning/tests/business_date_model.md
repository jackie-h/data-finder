# Legend Business Date Test Model

Source: legend-engine core_relational `milestoning/tests/milestoningModel.pure` (the Order, Product, ProductClassification,
ProductSynonym and Exchange subset the business date tests use). In Legend, Product, ProductClassification, ProductSynonym and Exchange are business
temporal and Order is not. Each relation has an accessor on one class only, so each is a one-way
association, apart from Order and Product, which Legend relates both ways (`orders`). OrderDescription is not ported.

## Sub-Domain: milestoning

### Class: Order

| Name  | Description |
|-------|-------------|
| Order |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Id       | id | Integer | 1            | Y   |             |

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: ProductClassification

| Name                  | Description |
|-----------------------|-------------|
| ProductClassification |             |

| Property    | Id          | Type   | Multiplicity | Key | Description |
|-------------|-------------|--------|--------------|-----|-------------|
| Type        | type        | String | 1            | Y   |             |
| Description | description | String | 1            |     |             |

### Class: ProductSynonym

| Name           | Description |
|----------------|-------------|
| ProductSynonym |             |

| Property | Id      | Type   | Multiplicity | Key | Description |
|----------|---------|--------|--------------|-----|-------------|
| Synonym  | synonym | String | 1            | Y   |             |
| Type     | type    | String | 1            |     |             |

### Class: Exchange

| Name     | Description |
|----------|-------------|
| Exchange |             |

| Property | Id   | Type   | Multiplicity | Key | Description |
|----------|------|--------|--------------|-----|-------------|
| Name     | name | String | 1            | Y   |             |

### Association: OrderProduct

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| OrderProduct | Order  | Product              | product            | *                   | Product | Orders               | orders             | *                   |             |

### Association: ProductClassificationLink

| Name                      | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target                | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------------|---------|----------------------|--------------------|---------------------|-----------------------|----------------------|--------------------|---------------------|-------------|
| ProductClassificationLink | Product | Classification       | classification     | 1                   | ProductClassification |                      |                    | *                   |             |

### Association: ProductSynonymLink

| Name               | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target         | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------------|---------|----------------------|--------------------|---------------------|----------------|----------------------|--------------------|---------------------|-------------|
| ProductSynonymLink | Product | Synonyms             | synonyms           | 1..*                | ProductSynonym |                      |                    | *                   |             |

### Association: ProductExchangeLink

| Name                | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------|---------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| ProductExchangeLink | Product | Exchange             | exchange           | 1                   | Exchange |                      |                    | *                   |             |

### Association: ClassificationExchangeLink

| Name                     | Source                | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------------------|-----------------------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| ClassificationExchangeLink | ProductClassification | Exchange           | exchange           | 1                   | Exchange |                      |                    | *                   |             |
