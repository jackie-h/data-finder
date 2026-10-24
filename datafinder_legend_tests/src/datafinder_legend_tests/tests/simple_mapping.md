# Legend Simple Relational Mapping

Source: legend-engine core_relational `tests/relationalSetUp.pure` (`dbInc` and `db` tables and the `simpleRelationalMapping` mapping, the Person, Firm, Address, Location, Trade,
Product, Synonym, Account, Order, Interaction and TradeEvent subset).

## Model: simple_model.md

## DataStore: dbInc (Database)

### Schema: main

#### Table: personTable → Person

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| FIRSTNAME | VARCHAR |  | Y | firstName |
| LASTNAME | VARCHAR |  | Y | lastName |
| AGE | INT |  | Y | age |
| ADDRESSID | INT | FK | Y | address |
| FIRMID | INT | FK | Y | firm |
| MANAGERID | INT | FK | Y | manager |

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

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| LEGALNAME | VARCHAR |  | Y | legalName |
| ADDRESSID | INT | FK | Y | address |

#### Association: FirmAddress

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ADDRESSID     | addressTable | ID            |

#### Table: addressTable → Address

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| NAME | VARCHAR |  | Y | name |
| STREET | VARCHAR |  | Y | street |
| COMMENTS | VARCHAR |  | Y | comments |

#### Table: locationTable → Location

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| PERSONID | INT | FK | Y | person |
| PLACE | VARCHAR |  | Y | place |
| date | DATE |  | Y | censusdate |

#### Association: PersonLocations

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PERSONID      | personTable  | ID            |

#### Table: accountTable → Account

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| name | VARCHAR |  | Y | name |
| createDate | DATE |  | Y | createDate |

#### Table: tradeTable → Trade

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| prodId | INT | FK | Y | product |
| accountID | INT | FK | Y | account |
| quantity | DOUBLE |  | Y | quantity |
| tradeDate | DATE |  | Y | date |
| settlementDateTime | TIMESTAMP |  | Y | settlementDateTime |

#### Association: TradeAccount

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| accountID | accountTable | ID |

#### Association: TradeProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| prodId | productTable | ID |

#### Table: orderTable → Order

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| accountID | INT | FK | Y | account |
| quantity | DOUBLE |  | Y | quantity |
| orderDate | DATE |  | Y | date |
| settlementDateTime | TIMESTAMP |  | Y | settlementDateTime |

#### Association: OrderAccount

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| accountID | accountTable | ID |

#### Table: tradeEventTable → TradeEvent

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| EVENT_ID | INT | PK | N | id |
| trade_id | INT | FK | Y | trade |
| eventType | VARCHAR |  | Y | eventType |
| eventDate | DATE |  | Y | date |
| person_id | INT | FK | Y | initiator |

#### Association: TradeEventTrade

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| trade_id | tradeTable | ID |

#### Association: TradeEventInitiator

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| person_id | personTable | ID |

#### Table: interactionTable → Interaction

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| sourceId | INT | FK | Y | source |
| targetId | INT | FK | Y | target |
| time | INT |  | Y | time |

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

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| NAME | VARCHAR |  | Y | name |

#### Table: synonymTable → Synonym

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| ID | INT | PK | N | id |
| PRODID | INT | FK | Y | product |
| TYPE | VARCHAR |  | Y | typeAsString |
| NAME | VARCHAR |  | Y | name |

#### Association: SynonymProduct

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| PRODID | productTable | ID |
