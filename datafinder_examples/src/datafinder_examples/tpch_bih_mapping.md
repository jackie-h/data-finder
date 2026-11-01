# TPCH BiH Mapping

## Model: tpch_bih_model.md

## DataStore: tpch_bih_db (Database)

| Scheme            | processing_start | processing_end | business_date | business_date_from      | business_date_to      |
|-------------------|------------------|----------------|---------------|-------------------------|-----------------------|
| system_time       | sys_time_begin   | sys_time_end   |               |                         |                       |
| visible_time      | sys_time_begin   | sys_time_end   |               | visible_time_begin      | visible_time_end      |
| availability_time | sys_time_begin   | sys_time_end   |               | availability_time_begin | availability_time_end |
| validity_time     | sys_time_begin   | sys_time_end   |               | validity_time_begin     | validity_time_end     |
| active_time       | sys_time_begin   | sys_time_end   |               | active_time_begin       | active_time_end       |

### Schema: tpch

#### Table: region → Region

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| r_regionkey | INT     | PK  | N        | regionKey   |
| r_name      | VARCHAR |     | N        | name        |
| r_comment              | VARCHAR   |     | Y        | comment      |

#### Table: nation → Nation

| Column      | Type    | Key | Nullable | Property ID |
|-------------|---------|-----|----------|-------------|
| n_nationkey | INT     | PK  | N        | nationKey   |
| n_name      | VARCHAR |     | N        | name        |
| n_regionkey | INT     | FK  | N        | region      |
| n_comment              | VARCHAR   |     | Y        | comment      |

#### Association: NationRegion

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| n_regionkey   | region       | r_regionkey   |

#### Table: supplier → Supplier (milestoning: system_time)

| Column         | Type      | Key | Nullable | Property ID |
|----------------|-----------|-----|----------|-------------|
| s_suppkey      | INT       | PK  | N        | supplierKey |
| s_name         | VARCHAR   |     | N        | name        |
| s_nationkey    | INT       | FK  | N        | nation      |
| s_acctbal      | DOUBLE    |     | N        | acctBal     |
| s_address              | VARCHAR   |     | Y        | address      |
| s_phone                | VARCHAR   |     | Y        | phone        |
| s_comment              | VARCHAR   |     | Y        | comment      |
| sys_time_begin | TIMESTAMP |     | N        | sysTimeBegin |
| sys_time_end   | TIMESTAMP |     | N        | sysTimeEnd  |

#### Association: SupplierNation

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| s_nationkey   | nation       | n_nationkey   |

#### Table: part → Part (milestoning: availability_time)

| Column                  | Type      | Key | Nullable | Property ID  |
|-------------------------|-----------|-----|----------|--------------|
| p_partkey               | INT       | PK  | N        | partKey      |
| p_name                  | VARCHAR   |     | N        | name         |
| p_mfgr                  | VARCHAR   |     | N        | mfgr         |
| p_brand                 | VARCHAR   |     | N        | brand        |
| p_type                  | VARCHAR   |     | N        | type         |
| p_size                  | INT       |     | N        | size         |
| p_container             | VARCHAR   |     | N        | container    |
| p_retailprice           | DOUBLE    |     | N        | retailPrice  |
| p_comment              | VARCHAR   |     | Y        | comment      |
| availability_time_begin | DATE      |     | N        | availabilityTimeBegin |
| availability_time_end   | DATE      |     | N        | availabilityTimeEnd   |
| sys_time_begin          | TIMESTAMP |     | N        | sysTimeBegin |
| sys_time_end            | TIMESTAMP |     | N        | sysTimeEnd   |

#### Table: partsupp → PartSupp (milestoning: validity_time)

| Column              | Type      | Key | Nullable | Property ID  |
|---------------------|-----------|-----|----------|--------------|
| ps_partkey          | INT       | FK  | N        | part         |
| ps_suppkey          | INT       | FK  | N        | supplier     |
| ps_availqty         | INT       |     | N        | availQty     |
| ps_supplycost       | DOUBLE    |     | N        | supplyCost   |
| ps_comment             | VARCHAR   |     | Y        | comment      |
| validity_time_begin | DATE      |     | N        | validityTimeBegin |
| validity_time_end   | DATE      |     | N        | validityTimeEnd   |
| sys_time_begin      | TIMESTAMP |     | N        | sysTimeBegin |
| sys_time_end        | TIMESTAMP |     | N        | sysTimeEnd   |

#### Association: PartSuppSamePart

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ps_partkey | partsupp | ps_partkey |

#### Association: PartSuppPart

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ps_partkey    | part         | p_partkey     |

#### Association: PartSuppSupplier

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| ps_suppkey    | supplier     | s_suppkey     |

#### Table: customer → Customer (milestoning: visible_time)

| Column             | Type      | Key | Nullable | Property ID  |
|--------------------|-----------|-----|----------|--------------|
| c_custkey          | INT       | PK  | N        | customerKey  |
| c_name             | VARCHAR   |     | N        | name         |
| c_nationkey        | INT       | FK  | N        | nation       |
| c_acctbal          | DOUBLE    |     | N        | acctBal      |
| c_mktsegment       | VARCHAR   |     | N        | mktSegment   |
| c_address              | VARCHAR   |     | Y        | address      |
| c_phone                | VARCHAR   |     | Y        | phone        |
| c_comment              | VARCHAR   |     | Y        | comment      |
| visible_time_begin | DATE      |     | N        | visibleTimeBegin |
| visible_time_end   | DATE      |     | N        | visibleTimeEnd   |
| sys_time_begin     | TIMESTAMP |     | N        | sysTimeBegin |
| sys_time_end       | TIMESTAMP |     | N        | sysTimeEnd   |

#### Association: CustomerOtherVersion

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| c_custkey | customer | c_custkey |

#### Association: CustomerNation

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| c_nationkey   | nation       | n_nationkey   |

#### Table: orders → Order (milestoning: active_time)

| Column                | Type      | Key | Nullable | Property ID         |
|-----------------------|-----------|-----|----------|---------------------|
| o_orderkey            | INT       | PK  | N        | orderKey            |
| o_custkey             | INT       | FK  | N        | customer            |
| o_orderstatus         | VARCHAR   |     | N        | orderStatus         |
| o_totalprice          | DOUBLE    |     | N        | totalPrice          |
| o_orderdate           | DATE      |     | N        | orderDate           |
| o_orderpriority       | VARCHAR   |     | N        | orderPriority       |
| o_clerk               | VARCHAR   |     | N        | clerk               |
| o_shippriority        | INT       |     | N        | shipPriority        |
| o_comment              | VARCHAR   |     | Y        | comment      |
| receivable_time_begin | DATE      |     | N        | receivableTimeBegin |
| receivable_time_end   | DATE      |     | N        | receivableTimeEnd   |
| active_time_begin     | DATE      |     | N        | activeTimeBegin     |
| active_time_end       | DATE      |     | N        | activeTimeEnd       |
| sys_time_begin        | TIMESTAMP |     | N        | sysTimeBegin        |
| sys_time_end          | TIMESTAMP |     | N        | sysTimeEnd          |

#### Association: OrderOtherVersion

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| o_orderkey | orders | o_orderkey |

#### Association: OrderCustomer

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| o_custkey     | customer     | c_custkey     |

#### Table: lineitem → LineItem (milestoning: active_time)

| Column            | Type      | Key | Nullable | Property ID     |
|-------------------|-----------|-----|----------|-----------------|
| l_orderkey        | INT       | FK  | N        | order           |
| l_linenumber      | INT       |     | N        | lineNumber      |
| l_partkey         | INT       | FK  | N        | part            |
| l_suppkey         | INT       | FK  | N        | supplier        |
| l_quantity        | DOUBLE    |     | N        | quantity        |
| l_extendedprice   | DOUBLE    |     | N        | extendedPrice   |
| l_discount        | DOUBLE    |     | N        | discount        |
| l_tax             | DOUBLE    |     | N        | tax             |
| l_returnflag      | VARCHAR   |     | N        | returnFlag      |
| l_linestatus      | VARCHAR   |     | N        | lineStatus      |
| l_shipdate        | DATE      |     | N        | shipDate        |
| l_commitdate      | DATE      |     | N        | commitDate      |
| l_receiptdate     | DATE      |     | N        | receiptDate     |
| l_shipinstruct         | VARCHAR   |     | Y        | shipInstruct |
| l_shipmode        | VARCHAR   |     | N        | shipMode        |
| l_comment              | VARCHAR   |     | Y        | comment      |
| active_time_begin | DATE      |     | N        | activeTimeBegin |
| active_time_end   | DATE      |     | N        | activeTimeEnd   |
| sys_time_begin    | TIMESTAMP |     | N        | sysTimeBegin    |
| sys_time_end      | TIMESTAMP |     | N        | sysTimeEnd      |

#### Association: LineItemOrder

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| l_orderkey    | orders       | o_orderkey    |

#### Association: LineItemPart

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| l_partkey     | part         | p_partkey     |

#### Association: LineItemSupplier

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| l_suppkey     | supplier     | s_suppkey     |
