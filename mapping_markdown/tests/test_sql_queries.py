"""A Query section of a mapping is read as SQL, checked against the tables and the columns it lists, and written back."""
import pytest

from mapping_markdown.markdown_mapping import load, save
from model.sql_query import SqlQueryError

_MODEL = "sql_queries_model.md"


def _mapping(tmp_path, query_sql, table_columns="| firm_id   | INT     | PK  | N        | id          |\n| firm_name | VARCHAR |     | Y        | legalName   |",
             query_name="FIRM_QUERY"):
    from datafinder_examples import example_path
    text = f"""# Queries

## Model: {example_path(_MODEL)}

## DataStore: db (Database)

### Schema: org

#### Table: people → Person

| Column     | Type    | Key | Nullable | Property ID |
|------------|---------|-----|----------|-------------|
| person_id  | INT     | PK  | N        | id          |
| first_name | VARCHAR |     | Y        | firstName   |
| firm_id    | INT     |     | Y        |             |
| firm_name  | VARCHAR |     | Y        |             |

#### Query: {query_name} → Firm

```sql
{query_sql}
```

| Column    | Type    | Key | Nullable | Property ID |
|-----------|---------|-----|----------|-------------|
{table_columns}
"""
    path = tmp_path / "mapping.md"
    path.write_text(text)
    return path


def _alias(mapping, class_name):
    rcm = next(r for r in mapping.mappings if r.clazz.name == class_name)
    return rcm.property_mappings[0].target.table  # type: ignore[attr-defined]


def test_a_query_is_read_into_a_table_alias_that_knows_its_sql(tmp_path):
    from model.relational import QuerySpecification, Table, TableAlias
    alias = _alias(load(str(_mapping(tmp_path, "SELECT DISTINCT firm_id, firm_name FROM people"))), "Firm")
    assert isinstance(alias, TableAlias) and alias.name == "FIRM_QUERY"
    assert isinstance(alias.relation, QuerySpecification) and alias.relation.distinct
    assert alias.sql == "SELECT DISTINCT firm_id, firm_name FROM people"
    assert [r.name for r in alias.reads] == ["people"] and isinstance(alias.reads[0], Table)
    assert [(c.name, c.type, c.primary_key) for c in alias.columns] == [("firm_id", "INT", True), ("firm_name", "VARCHAR", False)]  # type: ignore[attr-defined]
    assert all(c.owner is alias and c.table is alias for c in alias.columns)  # type: ignore[attr-defined]


def test_a_query_is_in_the_data_stores_queries_and_in_no_schema(tmp_path):
    from mapping_markdown.markdown_mapping import _build_repository_from_content, _md_parser
    from markdown_it.tree import SyntaxTreeNode
    repo = _build_repository_from_content(SyntaxTreeNode(_md_parser.parse(_mapping(tmp_path, "SELECT DISTINCT firm_id, firm_name FROM people").read_text())).children)
    assert repo is not None
    assert [q.name for q in repo.queries] == ["FIRM_QUERY"]
    assert [t.name for t in repo.schemas[0].tables] == ["people"]


def test_a_query_survives_writing(tmp_path):
    mapping = load(str(_mapping(tmp_path, "SELECT DISTINCT firm_id, firm_name FROM people")))
    from datafinder_examples import example_path
    path = tmp_path / "again.md"
    save(str(path), "Queries", mapping, [str(example_path(_MODEL))])
    text = path.read_text()
    assert "#### Query: FIRM_QUERY → Firm" in text and "```sql" in text and "### Queries" in text
    assert _alias(load(str(path)), "Firm").sql == "SELECT DISTINCT firm_id, firm_name FROM people"


@pytest.mark.parametrize("sql,message", [
    ("SELECT DISTINCT firm_id, firm_name FROM nowhere", "is not one of the tables there are"),
    ("SELECT DISTINCT firm_id, nope FROM people", "no table of the query has a column 'nope'"),
    ("SELEC firm_id FROM people", "cannot be read as postgres SQL"),
    ("SELECT firm_id FROM people", "the SQL gives the columns ['firm_id'] but its section lists"),
    ("SELECT firm_id, firm_name FROM people ORDER BY firm_id", "ORDER is not part of a mapped query"),
])
def test_a_query_that_cannot_be_read_or_does_not_match_its_columns_is_refused(tmp_path, sql, message):
    with pytest.raises((SqlQueryError, ValueError)) as error:
        load(str(_mapping(tmp_path, sql)))
    assert message in str(error.value)


def test_a_query_needs_its_sql_in_a_sql_block(tmp_path):
    path = _mapping(tmp_path, "X")
    path.write_text(path.read_text().replace("```sql\nX\n```", "SELECT 1"))
    with pytest.raises(ValueError, match="needs its SQL in a fenced"):
        load(str(path))


def test_every_query_of_the_example_mapping_survives_writing(tmp_path):
    from datafinder_examples import example_path

    def queries(mapping):
        return {r.clazz.name: r.property_mappings[0].target.table.sql  # type: ignore[attr-defined]
                for r in mapping.mappings if getattr(r.property_mappings[0].target.table, "sql", None)}  # type: ignore[attr-defined]

    mapping = load(str(example_path("sql_queries_mapping.md")))
    path = tmp_path / "again.md"
    save(str(path), "SQL Queries Mapping", mapping, [str(example_path(_MODEL))])
    again = load(str(path))
    assert queries(again) == queries(mapping)
    assert set(queries(mapping)) == {"Firm", "Position", "FirmXPerson", "Tag", "Shout", "Rate", "Quote"}


def test_a_query_is_milestoned_by_the_columns_its_sql_gives_and_the_scheme_survives_writing(tmp_path):
    from datafinder_examples import example_path
    from model.milestoning import BiTemporalColumns, BusinessDateRangeColumns  # noqa: F401
    mapping = load(str(example_path("sql_queries_mapping.md")))
    rate, quote = _alias(mapping, "Rate"), _alias(mapping, "Quote")
    assert rate.milestoning_scheme is not None and rate.milestoning_scheme.name == "business_date_range"
    assert quote.milestoning_scheme is not None and quote.milestoning_scheme.name == "bitemporal"
    assert quote.milestoning_scheme.processing_infinite_datetime == "9999-12-31 00:00:00"
    assert quote.milestoning_scheme.business_infinite_datetime == "9999-12-31"
    path = tmp_path / "again.md"
    save(str(path), "SQL Queries Mapping", mapping, [str(example_path(_MODEL))])
    text = path.read_text()
    assert "#### Query: RATE_QUERY → Rate (milestoning: business_date_range)" in text
    assert "#### Query: QUOTE_QUERY → Quote (milestoning: bitemporal)" in text
    again = load(str(path))
    assert _alias(again, "Quote").milestoning_scheme.name == "bitemporal"  # type: ignore[union-attr]
    # the scheme's columns are columns of the query, which the section lists
    assert {c.name for c in rate.columns} >= {"from_z", "thru_z"}  # type: ignore[attr-defined]
