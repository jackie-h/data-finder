# TPCH BiH Model

The TPC-H schema as used by the TPC-BiH bitemporal benchmark (Kaufmann et al., TPCTC 2013).
The comment columns and a few rarely used descriptive columns are left out; the temporal
columns are declared in the mapping, not here.

## Sub-Domain: tpch.bih

### Class: Region

| Name   | Description |
|--------|-------------|
| Region | Not versioned |

| Property   | Id        | Type    | Multiplicity | Key | Description |
|------------|-----------|---------|--------------|-----|-------------|
| Region Key | regionKey | Integer | 1            | Y   |             |
| Name       | name      | String  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: Nation

| Name   | Description |
|--------|-------------|
| Nation | Not versioned |

| Property   | Id        | Type    | Multiplicity | Key | Description |
|------------|-----------|---------|--------------|-----|-------------|
| Nation Key | nationKey | Integer | 1            | Y   |             |
| Name       | name      | String  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: Supplier

| Name     | Description |
|----------|-------------|
| Supplier | Degenerate: versioned in system time only |

| Property     | Id          | Type    | Multiplicity | Key | Description |
|--------------|-------------|---------|--------------|-----|-------------|
| Supplier Key | supplierKey | Integer | 1            | Y   |             |
| Name         | name        | String  | 1            |     |             |
| Acct Bal     | acctBal     | Double  | 1            |     |             |
| Address        | address   | String  | 1            |     |             |
| Phone          | phone     | String  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: Part

| Name | Description |
|------|-------------|
| Part | Bitemporal, application time is availability_time |

| Property     | Id          | Type    | Multiplicity | Key | Description |
|--------------|-------------|---------|--------------|-----|-------------|
| Part Key     | partKey     | Integer | 1            | Y   |             |
| Name         | name        | String  | 1            |     |             |
| Mfgr         | mfgr        | String  | 1            |     |             |
| Brand        | brand       | String  | 1            |     |             |
| Type         | type        | String  | 1            |     |             |
| Size         | size        | Integer | 1            |     |             |
| Container    | container   | String  | 1            |     |             |
| Retail Price | retailPrice | Double  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: PartSupp

| Name     | Description |
|----------|-------------|
| PartSupp | Bitemporal, application time is validity_time |

| Property    | Id         | Type    | Multiplicity | Key | Description |
|-------------|------------|---------|--------------|-----|-------------|
| Avail Qty   | availQty   | Integer | 1            |     |             |
| Supply Cost | supplyCost | Double  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: Customer

| Name     | Description |
|----------|-------------|
| Customer | Bitemporal, application time is visible_time |

| Property     | Id          | Type    | Multiplicity | Key | Description |
|--------------|-------------|---------|--------------|-----|-------------|
| Customer Key | customerKey | Integer | 1            | Y   |             |
| Name         | name        | String  | 1            |     |             |
| Acct Bal     | acctBal     | Double  | 1            |     |             |
| Mkt Segment  | mktSegment  | String  | 1            |     |             |
| Address        | address   | String  | 1            |     |             |
| Phone          | phone     | String  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: Order

| Name  | Description |
|-------|-------------|
| Order | Bitemporal with two application times: active_time is the designated one, receivable_time is an ordinary pair of dates |

| Property              | Id                  | Type    | Multiplicity | Key | Description |
|-----------------------|---------------------|---------|--------------|-----|-------------|
| Order Key             | orderKey            | Integer | 1            | Y   |             |
| Order Status          | orderStatus         | String  | 1            |     |             |
| Total Price           | totalPrice          | Double  | 1            |     |             |
| Order Date            | orderDate           | Date    | 1            |     |             |
| Order Priority        | orderPriority       | String  | 1            |     |             |
| Clerk                 | clerk               | String  | 1            |     |             |
| Ship Priority         | shipPriority        | Integer | 1            |     |             |
| Receivable Time Begin | receivableTimeBegin | Date    | 1            |     |             |
| Receivable Time End   | receivableTimeEnd   | Date    | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Class: LineItem

| Name     | Description |
|----------|-------------|
| LineItem | Bitemporal, application time is active_time |

| Property       | Id            | Type    | Multiplicity | Key | Description |
|----------------|---------------|---------|--------------|-----|-------------|
| Line Number    | lineNumber    | Integer | 1            | Y   |             |
| Quantity       | quantity      | Double  | 1            |     |             |
| Extended Price | extendedPrice | Double  | 1            |     |             |
| Discount       | discount      | Double  | 1            |     |             |
| Tax            | tax           | Double  | 1            |     |             |
| Return Flag    | returnFlag    | String  | 1            |     |             |
| Line Status    | lineStatus    | String  | 1            |     |             |
| Ship Date      | shipDate      | Date    | 1            |     |             |
| Commit Date    | commitDate    | Date    | 1            |     |             |
| Receipt Date   | receiptDate   | Date    | 1            |     |             |
| Ship Mode      | shipMode      | String  | 1            |     |             |
| Ship Instruct  | shipInstruct | String  | 1            |     |             |
| Comment        | comment   | String  | 1            |     |             |

### Association: NationRegion

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| NationRegion | Nation | Region               | region             | 1                   | Region | Nations              | nations            | *                   |             |

### Association: SupplierNation

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| SupplierNation | Supplier | Nation               | nation             | 1                   | Nation | Suppliers            | suppliers          | *                   |             |

### Association: CustomerNation

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| CustomerNation | Customer | Nation               | nation             | 1                   | Nation | Customers            | customers          | *                   |             |

### Association: PartSuppPart

| Name         | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PartSuppPart | PartSupp | Part                 | part               | 1                   | Part   | Part Supps           | partSupps          | *                   |             |

### Association: PartSuppSupplier

| Name             | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------|----------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| PartSuppSupplier | PartSupp | Supplier             | supplier           | 1                   | Supplier | Part Supps           | partSupps          | *                   |             |

### Association: OrderCustomer

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| OrderCustomer | Order  | Customer             | customer           | 1                   | Customer | Orders               | orders             | *                   |             |

### Association: LineItemOrder

| Name          | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| LineItemOrder | LineItem | Order                | order              | 1                   | Order  | Line Items           | lineItems          | *                   |             |

### Association: LineItemPart

| Name         | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| LineItemPart | LineItem | Part                 | part               | 1                   | Part   | Line Items           | lineItems          | *                   |             |

### Association: LineItemSupplier

| Name             | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target   | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------------|----------|----------------------|--------------------|---------------------|----------|----------------------|--------------------|---------------------|-------------|
| LineItemSupplier | LineItem | Supplier             | supplier           | 1                   | Supplier | Line Items           | lineItems          | *                   |             |

### Association: OrderOtherVersion

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| OrderOtherVersion | Order | Other Version | otherVersion | 0..1 | Order | | | * | The same order at another date: give the navigation its own business date or processing time |

### Association: CustomerOtherVersion

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| CustomerOtherVersion | Customer | Other Version | otherVersion | 0..1 | Customer | | | * | The same customer at another date: give the navigation its own business date or processing time |

### Association: PartSuppSamePart

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PartSuppSamePart | PartSupp | Same Part | samePart | 0..1 | PartSupp | | | * | The rows of the same part, for any supplier (a part and a supplier make a composite key, which an association cannot join on) |
