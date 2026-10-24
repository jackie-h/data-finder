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

| Name         | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description                  |
|--------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|------------------------------|
| TradeAccount | Trade  | Trades               | trades             | *                   | Account | Account              | account            | 1                   | Links a trade to its account |
```

| Column | Meaning |
|--------|---------|
| Source | Name of the source class |
| Source Property Name | Display name of the reverse navigation property added to the **target** class (`Account.trades`) |
| Source Property ID | camelCase id of the reverse navigation property (`trades`) |
| Source Multiplicity | `*` (many) or `1` (one) — multiplicity on the source side |
| Target | Name of the target class |
| Target Property Name | Display name of the forward navigation property added to the **source** class (`Trade.account`) |
| Target Property ID | camelCase id of the forward navigation property (`account`) |
| Target Multiplicity | `*` (many) or `1` (one) — multiplicity on the target side |

All eight fields (name and id for both properties, both multiplicities) are mandatory. The association is the single source of truth for cross-class navigation — do not also declare the same property explicitly on the class.

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

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
| ID        | INT     | PK  | N        | id          |
| ACCT_NAME | VARCHAR |     | Y        | name        |

### Schema: trading

#### Table: trades → Trade (milestoning: processing_only)

| Column     | Type      | Key | Nullable | Property ID |
|------------|-----------|-----|----------|-------------|
| sym        | VARCHAR   |     | Y        | symbol      |
| price      | DOUBLE    |     | Y        | price       |
| account_id | INT       | FK  | Y        | account     |
| in_z       | TIMESTAMP |     | Y        | validFrom   |
| out_z      | TIMESTAMP |     | Y        | validTo     |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
```

### Table mapping

`#### Table: <table_name> → <ClassName>` maps a database table to a class. The `Property ID` column references the property `Id` defined in the model.

Every column row needs a `Nullable` value: `Y` if the column can hold NULL, `N` if it is `NOT NULL`. A `PK` column is never nullable, so it should be `N`.

For foreign-key columns that represent association navigations, set `Key` to `FK` and use the association's `Target Property ID` as the property name. Follow the table block immediately with an `#### Association:` block that provides the join target.

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
uv run datafinder_duckdb/src/datafinder_duckdb/refresh_mapping.py path/to/finance.db finance_mapping.md
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

Combine filters with `and_` and `or_`. A chain reads left to right, each call taking everything before it as its
left side, so the first line below is `(symbol is AAPL or symbol is MSFT) and price > 100`:

```python
TradeFinder.symbol().eq("AAPL").or_(TradeFinder.symbol().eq("MSFT")).and_(TradeFinder.price() > 100)
TradeFinder.symbol().eq("AAPL").and_(TradeFinder.price() > 100).or_(TradeFinder.symbol().eq("MSFT"))
```

The second is `(symbol is AAPL and price > 100) or symbol is MSFT`. To group differently, build the part that should
bind first and combine it on the other side, e.g. `a.or_(b.and_(c))`.

Sort with `order_by`, and limit with `limit`. Several `order_by` calls accumulate: the latest call takes priority
and the earlier ones break its ties, so the two lines below sort by category and then by name. Sorting a column again
replaces its earlier direction.

```python
result = CompanyFinder.find_all(None, None, [CompanyFinder.name(), CompanyFinder.category()])
result.order_by(CompanyFinder.name().ascending()).order_by(CompanyFinder.category().ascending())
```

Sort by an aggregate the same way as by a column: `.order_by(CompanyFinder.id_().count().descending())`.

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

Reverse navigation — from Account to all its Trades (generated from `Source Property ID: trades`):

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

### Milestoned views

A query can be defined as a SQL view with no business date or processing time. Use the same `find_all` as
for any query, pass `None` for both dates, and call `to_view`. The view keeps the root table's milestoning
columns, so consumers choose the date when they query it.

```python
f = TradeFinder()
view = (f.find_all(None, None, [f.account().name(), f.symbol(), f.price()], f.is_settled().eq(True))
          .to_view("trade_enriched", "reporting"))

view.to_sql()                       # CREATE OR REPLACE VIEW reporting.trade_enriched AS ...
view.to_sql(dialect="databricks")   # transpiled with sqlglot
view.result().to_pandas()           # create the view on the registered backend and read its rows
```

Joins, the filter, `group_by`, aggregates and window functions all work as they do in `find_all`. Each column is
named after its display name in snake_case (`account_name`, `symbol`, `sum_price`), and the root's milestoning
columns (e.g. `in_z`, `out_z`) are added. Display names say how a column was reached, one relation per hop:
`f.account().branch().city()` is "Account Branch City" in a query result and `account_branch_city` in a view. In a grouped view they are grouped by too, so each group is a set of
rows with the same validity.

A view has no `ORDER BY` or row limit, so `to_view` rejects a query that has them. To order the rows you read
back, use `view.result().order_by("symbol")`.

**Joined milestoned tables are pinned to the root row.** Each is joined on the version that applied when the root
row was recorded (`child.in_z <= root.in_z AND child.out_z > root.in_z`), and on the root's business date where
there is one. Later changes to reference data never alter rows already in the view; a new version of the root
row picks up the new reference data. A bitemporal root uses `business_date_from` as its business date. All
joins are anchored to the root, including multi-hop ones.

`to_view` raises a `ViewDefinitionError` for:

- a query with a business date, a processing time, an `ORDER BY` or a row limit
- a joined table with a processing or business-date axis the root doesn't have
- projected columns that resolve to the same name, and view or schema names that aren't plain identifiers

Two limits to be aware of. Projecting across a to-many association (for example through a reverse accessor)
multiplies the root's rows; that isn't checked yet. And a joined table is assumed never to have overlapping
versions for one key at the same time.

#### Liquibase

`datafinder_liquibase` turns views into a Liquibase formatted-SQL changelog:

```python
from datafinder_liquibase import write_changelog, to_liquibase_master_changelog

write_changelog([view], "db/changelog/views.sql", author="jackie", dialect="databricks")
```

Each view becomes one changeset with `runOnChange:true`, id `<schema>.<view>`, and a `DROP VIEW IF EXISTS`
rollback, so changing a projection redeploys the view without a new id.
`to_liquibase_master_changelog(["views.sql"])` returns a YAML master that includes the changelog.

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
| `datafinder_liquibase/` | Liquibase changelog output for milestoned views |
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
