# Legend Simple Test Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (the Person, Firm, Address, Location, Trade, Product, Synonym, Account, Order, Interaction and TradeEvent subset,
without enumerations, qualified properties or the properties computed from database views).

## Sub-Domain: simple

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| FirstName | firstName | String  | 1            |     |             |
| LastName  | lastName  | String  | 1            |     |             |
| Age       | age       | Integer | 0..1         |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| LegalName | legalName | String  | 1            |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id       | Type    | Multiplicity | Key | Description |
|----------|----------|---------|--------------|-----|-------------|
| Id       | id       | Integer | 1            | Y   |             |
| Name     | name     | String  | 1            |     |             |
| Street   | street   | String  | 0..1         |     |             |
| Comments | comments | String  | 0..1         |     |             |

### Class: Location

| Name     | Description |
|----------|-------------|
| Location |             |

| Property   | Id         | Type    | Multiplicity | Key | Description |
|------------|------------|---------|--------------|-----|-------------|
| Id         | id         | Integer | 1            | Y   |             |
| Place      | place      | String  | 1            |     |             |
| Censusdate | censusdate | Date    | 0..1         |     |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Firm                 | firm               | 0..1                | Firm   | Employees            | employees          | *                   |             |

### Association: PersonAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person | Address              | address            | 0..1                | Address |                      |                    | *                   |             |

### Association: PersonManager

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonManager | Person | Manager              | manager            | 0..1                | Person |                      |                    | *                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Address              | address            | 0..1                | Address |                      |                    | *                   |             |

### Association: PersonLocations

| Name            | Source | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|--------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| PersonLocations | Person | Locations            | locations          | *                   | Location |                      |                    | 0..1                |             |

### Class: Product

| Name    | Description |
|---------|-------------|
| Product |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: Synonym

| Name    | Description |
|---------|-------------|
| Synonym |             |

| Property     | Id           | Type    | Multiplicity | Key | Description |
|--------------|--------------|---------|--------------|-----|-------------|
| Id           | id           | Integer | 1            | Y   |             |
| Name         | name         | String  | 1            |     |             |
| TypeAsString | typeAsString | String  | 1            |     |             |

### Class: Account

| Name    | Description |
|---------|-------------|
| Account |             |

| Property   | Id         | Type    | Multiplicity | Key | Description |
|------------|------------|---------|--------------|-----|-------------|
| Id         | id         | Integer | 1            | Y   |             |
| Name       | name       | String  | 1            |     |             |
| CreateDate | createDate | Date    | 1            |     |             |

### Class: Trade

| Name  | Description |
|-------|-------------|
| Trade |             |

| Property           | Id                 | Type     | Multiplicity | Key | Description |
|--------------------|--------------------|----------|--------------|-----|-------------|
| Id                 | id                 | Integer  | 1            | Y   |             |
| Quantity           | quantity           | Double   | 1            |     |             |
| Date               | date               | Date     | 1            |     |             |
| SettlementDateTime | settlementDateTime | DateTime | 0..1         |     |             |

### Class: Order

| Name  | Description |
|-------|-------------|
| Order |             |

| Property           | Id                 | Type     | Multiplicity | Key | Description |
|--------------------|--------------------|----------|--------------|-----|-------------|
| Id                 | id                 | Integer  | 1            | Y   |             |
| Quantity           | quantity           | Double   | 1            |     |             |
| Date               | date               | Date     | 1            |     |             |
| SettlementDateTime | settlementDateTime | DateTime | 0..1         |     |             |

### Class: Interaction

| Name        | Description |
|-------------|-------------|
| Interaction |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | String  | 1            |     |             |
| Time     | time | Integer | 1            |     |             |

### Class: TradeEvent

| Name       | Description |
|------------|-------------|
| TradeEvent |             |

| Property  | Id        | Type    | Multiplicity | Key | Description |
|-----------|-----------|---------|--------------|-----|-------------|
| Id        | id        | Integer | 1            | Y   |             |
| EventType | eventType | String  | 0..1         |     |             |
| Date      | date      | Date    | 1            |     |             |

### Association: TradeAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| TradeAccount | Trade  | Account              | account            | 0..1                | Account | Trades               | trades             | *                   |             |

### Association: TradeProduct

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| TradeProduct | Trade  | Product              | product            | 0..1                | Product |                      |                    | *                   |             |

### Association: TradeEventTrade

| Name            | Source | Source Property Name | Source Property ID | Source Multiplicity | Target     | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|--------|----------------------|--------------------|---------------------|------------|----------------------|--------------------|---------------------|-------------|
| TradeEventTrade | Trade  | Events               | events             | *                   | TradeEvent |                      |                    | 0..1                |             |

### Association: TradeEventInitiator

| Name                | Source     | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------------|------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| TradeEventInitiator | TradeEvent | Initiator            | initiator          | 0..1                | Person |                      |                    | *                   |             |

### Association: OrderAccount

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| OrderAccount | Order  | Account              | account            | 0..1                | Account | Orders               | orders             | *                   |             |

### Association: SynonymProduct

| Name           | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|---------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| SynonymProduct | Synonym | Product              | product            | 1                   | Product | Synonyms             | synonyms           | *                   |             |

### Association: InteractionSource

| Name              | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| InteractionSource | Interaction | Source               | source             | 0..1                | Person |                      |                    | *                   |             |

### Association: InteractionTarget

| Name              | Source      | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------------|-------------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| InteractionTarget | Interaction | Target               | target             | 0..1                | Person |                      |                    | *                   |             |
