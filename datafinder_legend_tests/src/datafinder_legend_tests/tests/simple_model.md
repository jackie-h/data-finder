# Legend Simple Test Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (the Person, Firm, Address, Location, Trade, Product, Synonym, Account, Order, Interaction and TradeEvent subset,
without enumerations, qualified properties or the properties computed from database views).

## Sub-Domain: simple

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type    | Key | Description |
|-----------|-----------|---------|-----|-------------|
| Id        | id        | Integer | Y   |             |
| FirstName | firstName | String  |     |             |
| LastName  | lastName  | String  |     |             |
| Age       | age       | Integer |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type    | Key | Description |
|-----------|-----------|---------|-----|-------------|
| Id        | id        | Integer | Y   |             |
| LegalName | legalName | String  |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id     | Type    | Key | Description |
|----------|--------|---------|-----|-------------|
| Id       | id     | Integer | Y   |             |
| Name     | name   | String  |     |             |
| Street   | street | String  |     |             |
| Comments | comments | String |    |             |

### Class: Location

| Name     | Description |
|----------|-------------|
| Location |             |

| Property | Id    | Type    | Key | Description |
|----------|-------|---------|-----|-------------|
| Id       | id    | Integer | Y   |             |
| Place    | place | String  |     |             |
| Censusdate | censusdate | Date |  |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Employees            | employees          | *                   | Firm   | Firm                 | firm               | 1                   |             |

### Association: PersonAddress

| Name          | Source  | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|---------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person  | Persons              | persons            | *                   | Address | Address             | address            | 1                   |             |

### Association: PersonManager

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonManager | Person | Reports              | reports            | *                   | Person | Manager              | manager            | 1                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Firms                | firms              | *                   | Address | Address              | address            | 1                   |             |

### Association: PersonLocations

| Name            | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonLocations | Location | Locations            | locations          | *                   | Person | Person               | person             | 1                   |             |

### Class: Product

| Name | Description |
|------|-------------|
| Product |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| Name | name | String |  |             |

### Class: Synonym

| Name | Description |
|------|-------------|
| Synonym |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| Name | name | String |  |             |
| TypeAsString | typeAsString | String |  |             |

### Class: Account

| Name | Description |
|------|-------------|
| Account |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| Name | name | String |  |             |
| CreateDate | createDate | Date |  |             |

### Class: Trade

| Name | Description |
|------|-------------|
| Trade |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| Quantity | quantity | Double |  |             |
| Date | date | Date |  |             |
| SettlementDateTime | settlementDateTime | DateTime |  |             |

### Class: Order

| Name | Description |
|------|-------------|
| Order |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| Quantity | quantity | Double |  |             |
| Date | date | Date |  |             |
| SettlementDateTime | settlementDateTime | DateTime |  |             |

### Class: Interaction

| Name | Description |
|------|-------------|
| Interaction |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | String |  |             |
| Time | time | Integer |  |             |

### Class: TradeEvent

| Name | Description |
|------|-------------|
| TradeEvent |             |

| Property | Id | Type | Key | Description |
|----------|----|------|-----|-------------|
| Id | id | Integer | Y |             |
| EventType | eventType | String |  |             |
| Date | date | Date |  |             |

### Association: TradeAccount

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| TradeAccount | Trade | Trades | trades | * | Account | Account | account | 1 |             |

### Association: TradeProduct

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| TradeProduct | Trade | Trades | trades | * | Product | Product | product | 1 |             |

### Association: TradeEventTrade

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| TradeEventTrade | TradeEvent | Events | events | * | Trade | Trade | trade | 1 |             |

### Association: TradeEventInitiator

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| TradeEventInitiator | TradeEvent | InitiatedEvents | initiatedEvents | * | Person | Initiator | initiator | 1 |             |

### Association: OrderAccount

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| OrderAccount | Order | Orders | orders | * | Account | Account | account | 1 |             |

### Association: SynonymProduct

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| SynonymProduct | Synonym | Synonyms | synonyms | * | Product | Product | product | 1 |             |

### Association: InteractionSource

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| InteractionSource | Interaction | SourceInteractions | sourceInteractions | * | Person | Source | source | 1 |             |

### Association: InteractionTarget

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| InteractionTarget | Interaction | TargetInteractions | targetInteractions | * | Person | Target | target | 1 |             |
