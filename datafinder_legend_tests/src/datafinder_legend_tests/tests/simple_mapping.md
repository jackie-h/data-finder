# Legend Simple Relational Mapping

Source: legend-engine core_relational `tests/relationalSetUp.pure` (`dbInc` and `db` tables and the `simpleRelationalMapping` mapping, the Person, Firm, Address, Location, Trade,
Product, Synonym, Account, Order, Interaction and TradeEvent subset).

## Model: simple_model.md

## DataStore: dbInc (Database)

### Schema: main

#### Table: personTable → Person

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| FIRSTNAME | VARCHAR |     | firstName   |
| LASTNAME  | VARCHAR |     | lastName    |
| AGE       | INT     |     | age         |
| ADDRESSID | INT     | FK  | address     |
| FIRMID    | INT     | FK  | firm        |
| MANAGERID | INT     | FK  | manager     |

#### Association: PersonAddress

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID     | addressTable | ID            |

#### Association: PersonFirm

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| FIRMID        | firmTable    | ID            |

#### Association: PersonManager

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| MANAGERID     | personTable  | ID            |

#### Table: firmTable → Firm

| Column    | Type    | Key | Property ID |
|-----------|---------|-----|-------------|
| ID        | INT     | PK  | id          |
| LEGALNAME | VARCHAR |     | legalName   |
| ADDRESSID | INT     | FK  | address     |

#### Association: FirmAddress

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID     | addressTable | ID            |

#### Table: addressTable → Address

| Column   | Type    | Key | Property ID |
|----------|---------|-----|-------------|
| ID       | INT     | PK  | id          |
| NAME     | VARCHAR |     | name        |
| STREET   | VARCHAR |     | street      |
| COMMENTS | VARCHAR |     | comments    |

#### Table: locationTable → Location

| Column   | Type    | Key | Property ID |
|----------|---------|-----|-------------|
| ID       | INT     | PK  | id          |
| PERSONID | INT     | FK  | person      |
| PLACE    | VARCHAR |     | place       |
| date     | DATE    |     | censusdate  |

#### Association: PersonLocations

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PERSONID      | personTable  | ID            |

#### Table: accountTable → Account

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| name | VARCHAR |  | name |
| createDate | DATE |  | createDate |

#### Table: tradeTable → Trade

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| prodId | INT | FK | product |
| accountID | INT | FK | account |
| quantity | DOUBLE |  | quantity |
| tradeDate | DATE |  | date |
| settlementDateTime | TIMESTAMP |  | settlementDateTime |

#### Association: TradeAccount

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| accountID | accountTable | ID |

#### Association: TradeProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodId | productTable | ID |

#### Table: orderTable → Order

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| accountID | INT | FK | account |
| quantity | DOUBLE |  | quantity |
| orderDate | DATE |  | date |
| settlementDateTime | TIMESTAMP |  | settlementDateTime |

#### Association: OrderAccount

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| accountID | accountTable | ID |

#### Table: tradeEventTable → TradeEvent

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| EVENT_ID | INT | PK | id |
| trade_id | INT | FK | trade |
| eventType | VARCHAR |  | eventType |
| eventDate | DATE |  | date |
| person_id | INT | FK | initiator |

#### Association: TradeEventTrade

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| trade_id | tradeTable | ID |

#### Association: TradeEventInitiator

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| person_id | personTable | ID |

#### Table: interactionTable → Interaction

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| sourceId | INT | FK | source |
| targetId | INT | FK | target |
| time | INT |  | time |

#### Association: InteractionSource

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| sourceId | personTable | ID |

#### Association: InteractionTarget

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| targetId | personTable | ID |

### Schema: productSchema

#### Table: productTable → Product

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| NAME | VARCHAR |  | name |

#### Table: synonymTable → Synonym

| Column | Type | Key | Property ID |
|--------|------|-----|-------------|
| ID | INT | PK | id |
| PRODID | INT | FK | product |
| TYPE | VARCHAR |  | typeAsString |
| NAME | VARCHAR |  | name |

#### Association: SynonymProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PRODID | productTable | ID |
