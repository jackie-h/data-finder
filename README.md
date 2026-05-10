# Data Finder

Data Finder is a model-driven query library for Python. You define your domain model and map it to a relational schema; the library generates type-safe Python *Finder* classes that let you query your data without writing SQL. Temporal versioning (milestoning) is a first-class concept, not an afterthought.

## Overview

The workflow has three steps:

1. **Define a model** — describe your domain as classes and associations in Markdown.
2. **Define a mapping** — map each class property to a database table column in Markdown.
3. **Generate finders** — run the generator once to produce Python query classes.

Generated finders expose every mapped property as a typed method, and queries are assembled from those methods rather than from raw strings.

```python
from finance.trade.trade_finder import TradeFinder

results = TradeFinder.find_all(
    processing_valid_at=datetime.datetime.now(),
    display_columns=[TradeFinder.symbol(), TradeFinder.price()],
    filter_op=TradeFinder.symbol().eq("AAPL"),
)
df = results.to_pandas()
```

---

## Concepts

### Class

A **Class** represents a logical entity — the equivalent of a UML class or a business object. It has a name, an optional description, and a set of properties.

```markdown
### Class: Account

| Name    | Description                                     |
|---------|-------------------------------------------------|
| Account | Trading account used to buy and sell securities |

| Property | Id   | Type    | Key | Description |
|----------|------|---------|-----|-------------|
| Id       | id   | Integer | Y   |             |
| Name     | name | String  |     |             |
```

Each property has:

| Column | Meaning |
|--------|---------|
| Property | Human-readable label used in generated docstrings |
| Id | Machine identifier used in code and mappings |
| Type | A primitive type or another class name (for navigations defined via Associations) |
| Key | `Y` marks the business key |
| Description | Optional documentation |

Supported primitive types: `String`, `Integer`, `Double`, `Float`, `Decimal`, `Date`, `DateTime`, `Boolean`.

Classes belong to a **Sub-Domain** (package), which controls the Python package structure of the generated finders.

```markdown
## Sub-Domain: finance.reference_data

### Class: Account
...
```

A sub-domain with a dotted name (`finance.reference_data`) generates a nested package hierarchy.

### Inheritance

A class can extend one or more parent classes:

```markdown
### Class: Employee extends Person, Contactable
```

All inherited properties are available via `all_properties()` and appear in the generated finder.

### Association

An **Association** declares a named relationship between two classes. Both ends carry an explicit property name — the name that will appear as a navigation method in the generated finder.

```markdown
### Association: TradeAccount

| Name         | Source | Source Property | Source Multiplicity | Target  | Target Property | Target Multiplicity | Description                  |
|--------------|--------|-----------------|---------------------|---------|-----------------|---------------------|------------------------------|
| TradeAccount | Trade  | trades          | *                   | Account | account         | 1                   | Links a trade to its account |
```

| Column | Meaning |
|--------|---------|
| Source | Name of the source class |
| Source Property | Property name added to the **target** class for the reverse navigation (`Account.trades`) |
| Source Multiplicity | `*` (many) or `1` (one) — multiplicity on the source side |
| Target | Name of the target class |
| Target Property | Property name added to the **source** class for the forward navigation (`Trade.account`) |
| Target Multiplicity | `*` (many) or `1` (one) — multiplicity on the target side |

All four fields (both properties and both multiplicities) are mandatory. The association is the single source of truth for cross-class navigation — do not also declare the same property explicitly on the class.

When an association is loaded, both navigation properties are automatically available via `all_properties()` on the relevant classes and generate methods in the finder.

---

## Mapping

A **Mapping** file connects the logical model to the physical relational schema. It references one or more model files, declares the data store, and maps each class to a table.

### Structure

```markdown
# Finance Mapping

## Model: finance.md
## Model: finance_trade.md

## DataStore: finance_db (Database)

| Scheme                   | processing_start | processing_end | business_date |
|--------------------------|------------------|----------------|---------------|
| processing_only          | in_z             | out_z          |               |
| business_date_processing | in_z             | out_z          | DATE          |

### Schema: ref_data

#### Table: account_master → Account

| Column    | Type    | Key | Property |
|-----------|---------|-----|----------|
| ID        | INT     | PK  | id       |
| ACCT_NAME | VARCHAR |     | name     |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Property   |
|------------|-----------|-----|------------|
| sym        | VARCHAR   |     | symbol     |
| price      | DOUBLE    |     | price      |
| account_id | INT       | FK  | account    |
| in_z       | TIMESTAMP |     | valid_from |
| out_z      | TIMESTAMP |     | valid_to   |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
```

### Table mapping

`#### Table: <table_name> → <ClassName>` maps a database table to a class. The property column references the property `Id` defined in the model.

For foreign-key columns that represent association navigations, set `Key` to `FK` and use the association's `Target Property` id as the property name. Follow the table block immediately with an `#### Association:` block that provides the join target.

### Association mapping

`#### Association: <AssociationName>` specifies how the foreign key joins to the target table.

| Column | Meaning |
|--------|---------|
| Source Column | The FK column in the source table |
| Target Table | The table the FK references |
| Target Column | The primary key column in the target table |

### Milestoning

Milestoning lets you query data as it was at a specific point in time. Declare named milestoning schemes on the `DataStore` and reference them per table.

| Scheme | Required columns | find_all signature |
|--------|------------------|--------------------|
| `processing_only` | `processing_start`, `processing_end` | `processing_valid_at` |
| `business_date` | `business_date` | `business_date`, `processing_valid_at` |
| `business_date_processing` | `processing_start`, `processing_end`, `business_date` | `business_date`, `processing_valid_at` |
| `bitemporal` | `processing_start`, `processing_end`, `business_date_from`, `business_date_to` | `business_date`, `processing_valid_at` |

Tag a table with the scheme name in parentheses:

```markdown
#### Table: trades → Trade (milestoning: processing_only)
```

---

## Generating Finders

Load the mapping markdown and call `generate`:

```python
from mapping_markdown.markdown_mapping import load
from datafinder_generator.generator import generate

mapping = load("finance_mapping.md")
generate(mapping, output_directory="src/")
```

This writes one `<classname>_finder.py` file per mapped class, organised into the package structure declared by the sub-domain names.

### Refreshing a mapping from a live database

If you have a DuckDB database you can auto-populate the mapping skeleton from the live schema:

```bash
uv run example/refresh_mapping.py path/to/finance.db finance_mapping.md
```

---

## Using the Generated Finders

### Simple queries

```python
from finance.reference_data.account_finder import AccountFinder

# All accounts
result = AccountFinder.find_all(
    display_columns=[AccountFinder.id(), AccountFinder.name()],
)

# Filtered
result = AccountFinder.find_all(
    display_columns=[AccountFinder.id(), AccountFinder.name()],
    filter_op=AccountFinder.name().eq("Acme Corp"),
)
df = result.to_pandas()
```

### Temporal queries

For milestoned tables, supply the temporal arguments:

```python
import datetime
from finance.trade.trade_finder import TradeFinder

# Trades as of now (processing_only milestoning)
result = TradeFinder.find_all(
    processing_valid_at=datetime.datetime.now(),
    display_columns=[TradeFinder.symbol(), TradeFinder.price()],
    filter_op=TradeFinder.symbol().eq("AAPL"),
)

# Positions on a specific business date as processed at a specific time
from finance.trade.contractualposition_finder import ContractualPositionFinder

result = ContractualPositionFinder.find_all(
    business_date=datetime.date(2023, 1, 15),
    processing_valid_at=datetime.datetime(2023, 1, 16),
    display_columns=[ContractualPositionFinder.quantity(), ContractualPositionFinder.npv()],
)
```

### Navigating associations

Forward navigation — from Trade to its Account:

```python
result = TradeFinder.find_all(
    processing_valid_at=datetime.datetime.now(),
    display_columns=[
        TradeFinder.symbol(),
        TradeFinder.account().name(),   # navigates the TradeAccount association
    ],
)
```

Reverse navigation — from Account to all its Trades (generated from `Source Property: trades`):

```python
result = AccountFinder.find_all(
    display_columns=[
        AccountFinder.name(),
        AccountFinder.trades().symbol(),
        AccountFinder.trades().price(),
    ],
)
```

### Result formats

```python
result.to_pandas()   # pandas DataFrame
result.to_numpy()    # NumPy array
```

---

## Project Layout

| Directory | Contents |
|-----------|----------|
| `model/` | Core metamodel (`Class`, `Property`, `Association`, `Package`) |
| `model_markdown/` | Load and save model definitions from Markdown |
| `mapping_markdown/` | Load and save mapping definitions from Markdown |
| `datafinder/` | Query engine — `FinderResult`, filter operations, typed attributes |
| `datafinder_generator/` | Jinja2-based code generator that produces Finder classes |
| `datafinder_ibis/` | Ibis backend for query execution |
| `datafinder_ibis_duckdb/` | DuckDB integration via Ibis |
| `datafinder_duckdb/` | Schema reader — introspects a live DuckDB database |
| `example/` | End-to-end example with finance model, mapping, and queries |

---

## Development

This project uses `uv`. To run the tests:

```bash
uv run pytest
```

To add a dependency to a sub-project:

```bash
uv add --optional <group> <package>
```
