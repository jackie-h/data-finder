"""A class can be mapped to several queries, each its own named set, which the mapping puts together as a union and checks."""
import pytest

from mapping_markdown.markdown_mapping import load, save
from model.relational_mapping import RelationalClassMapping

_MODEL = "sql_queries_model.md"
_COLUMNS = """| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        | id          |
| name   | VARCHAR |     | Y        | legalName   |
"""


def _query(name, heading_options, sql="SELECT id, name FROM big_firms", columns=_COLUMNS, cls="Firm"):
    return f"""#### Query: {name} → {cls}{heading_options}

```sql
{sql}
```

{columns}
"""


_DEFAULT = _query("BIG", " (set: big)") + "\n" + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms")


def _mapping(tmp_path, queries=_DEFAULT, extra_tables=""):
    from datafinder_examples import example_path
    text = f"""# Sets

## Model: {example_path(_MODEL)}

## DataStore: db (Database)

### Schema: org

#### Table: big_firms

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |
| region | VARCHAR |     | Y        |             |
| from_z | DATE    |     | Y        |             |
| thru_z | DATE    |     | Y        |             |

#### Table: small_firms

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |
| from_z | DATE    |     | Y        |             |
| thru_z | DATE    |     | Y        |             |

#### Table: regions

| Column | Type    | Key | Nullable | Property ID |
|--------|---------|-----|----------|-------------|
| id     | INT     | PK  | N        |             |
| name   | VARCHAR |     | Y        |             |

{extra_tables}
{queries}
"""
    path = tmp_path / "mapping.md"
    path.write_text(text)
    return path


def _firm(mapping) -> RelationalClassMapping:
    firm = next(r for r in mapping.mappings if r.clazz.name == "Firm")
    assert isinstance(firm, RelationalClassMapping)
    return firm


# --- the sets are read, and named ---------------------------------------------------------------------------------------------------------

def test_the_sets_of_a_class_are_one_class_mapping_that_keeps_its_sets(tmp_path):
    firm = _firm(load(str(_mapping(tmp_path))))
    assert [s.set_name for s in firm.sets] == ["big", "small"]
    assert firm.set_name is None
    assert [s.main_relation.name for s in firm.sets] == ["BIG", "SMALL"]  # type: ignore[union-attr]


def test_the_union_of_the_sets_is_one_query_with_the_number_of_the_set_each_row_came_from(tmp_path):
    from model.relational import UnionOperation
    firm = _firm(load(str(_mapping(tmp_path))))
    union = firm.main_relation
    assert isinstance(union.relation, UnionOperation) and union.relation.all  # type: ignore[union-attr]
    assert [c.name for c in union.columns] == ["id", "name", "_set"]  # type: ignore[union-attr]
    assert [pm.target.name for pm in firm.property_mappings] == ["id", "name"]  # type: ignore[attr-defined]
    assert all(pm.target.table is union for pm in firm.property_mappings)  # type: ignore[attr-defined]


# --- names are unique, and a class that has sets has them all named ----------------------------------------------------------------------

@pytest.mark.parametrize("queries,message", [
    # the same set name twice, of one class and of two
    (_query("BIG", " (set: s)") + _query("SMALL", " (set: s)", "SELECT id, name FROM small_firms"), "the set 's' is already the name of a set"),
    (_query("BIG", " (set: s)") + _query("OTHER", " (set: S)", cls="Person"), "the set 'S' is already the name of a set"),
    # the same query name twice, or a table's name
    (_query("BIG", " (set: a)") + _query("BIG", " (set: b)", "SELECT id, name FROM small_firms"), "the query 'BIG' is already the name of a query"),
    (_query("big_firms", " (set: a)") + _query("SMALL", " (set: b)", "SELECT id, name FROM small_firms"), "'big_firms' is already the name of a table"),
    # a name that is not an identifier
    (_query("BIG", " (set: two words)") + _query("SMALL", " (set: b)", "SELECT id, name FROM small_firms"), "is not a name"),
    # several queries of a class, and one has no set
    (_query("BIG", " (set: a)") + _query("SMALL", "", "SELECT id, name FROM small_firms"), "Firm is mapped to several queries: each needs a set"),
    (_query("BIG", "") + _query("SMALL", "", "SELECT id, name FROM small_firms"), "Firm is mapped to several queries: each needs a set"),
    # an option that is not one
    (_query("BIG", " (sett: a)"), "is not a heading option"),
])
def test_names_are_unique_and_every_set_of_a_class_is_named(tmp_path, queries, message):
    with pytest.raises(ValueError, match=message):
        load(str(_mapping(tmp_path, queries)))


def test_a_class_is_not_mapped_to_a_table_and_to_a_query(tmp_path):
    table = """#### Table: firms → Firm

| Column | Type | Key | Nullable | Property ID |
|--------|------|-----|----------|-------------|
| id     | INT  | PK  | N        | id          |

"""
    with pytest.raises(ValueError, match="Firm is mapped to a table and to a query"):
        load(str(_mapping(tmp_path, table + _query("BIG", " (set: a)"))))


# --- the sets must agree ------------------------------------------------------------------------------------------------------------------

def test_the_sets_must_give_the_same_columns(tmp_path):
    other = _query("SMALL", " (set: small)", "SELECT id, name AS label FROM small_firms", columns=_COLUMNS.replace("| name   |", "| label  |"))
    with pytest.raises(ValueError, match=r"the sets 'big' and 'small' of Firm give different columns: \['id', 'name'\] and \['id', 'label'\]"):
        load(str(_mapping(tmp_path, _query("BIG", " (set: big)") + other)))


def test_the_sets_must_map_the_same_properties_to_the_same_columns(tmp_path):
    swapped = _COLUMNS.replace("| legalName   |", "|             |").replace("| id          |", "| legalName   |")
    other = _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms", columns=swapped)
    with pytest.raises(ValueError, match="the sets 'big' and 'small' of Firm map the columns to different properties"):
        load(str(_mapping(tmp_path, _query("BIG", " (set: big)") + other)))


def test_the_sets_must_give_the_same_types(tmp_path):
    other = _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms", columns=_COLUMNS.replace("| INT     |", "| VARCHAR |"))
    with pytest.raises(ValueError, match="give different types"):
        load(str(_mapping(tmp_path, _query("BIG", " (set: big)") + other)))


def test_a_set_cannot_give_the_column_that_says_which_set_a_row_came_from(tmp_path):
    cols = _COLUMNS + "| _set   | VARCHAR |     | Y        |             |\n"
    big = _query("BIG", " (set: big)", "SELECT id, name, 'x' AS _set FROM big_firms", columns=cols)
    small = _query("SMALL", " (set: small)", "SELECT id, name, 'x' AS _set FROM small_firms", columns=cols)
    with pytest.raises(ValueError, match="_set is the column that says which set a row came from"):
        load(str(_mapping(tmp_path, big + small)))


# --- the key: what tells one row of the union from another, and why it is one -----------------------------------------------------------

def test_every_set_of_a_class_says_its_key(tmp_path):
    keyless = _COLUMNS.replace("| PK  |", "|     |")
    with pytest.raises(ValueError, match="the set 'big' of Firm has no key column"):
        load(str(_mapping(tmp_path, _query("BIG", " (set: big)", columns=keyless) + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms"))))


def test_a_set_has_the_key_of_its_table(tmp_path):
    """Selecting name and not the table's key: two rows of the set can look the same, so the rows of the union cannot be told apart."""
    cols = "| Column | Type    | Key | Nullable | Property ID |\n|---|---|---|---|---|\n| name | VARCHAR | PK | N | legalName |\n"
    big = _query("BIG", " (set: big)", "SELECT name FROM big_firms", columns=cols)
    small = _query("SMALL", " (set: small)", "SELECT name FROM small_firms", columns=cols)
    with pytest.raises(ValueError, match=r"the set 'big' of Firm does not select the key of big_firms \(id\)"):
        load(str(_mapping(tmp_path, big + small)))


def test_a_join_in_a_set_is_to_the_key_of_what_it_joins(tmp_path):
    """Joined on the region's name, a firm can match several regions: its row repeats, and so does its key."""
    sql = "SELECT b.id, b.name FROM big_firms b INNER JOIN regions r ON r.name = b.region"
    with pytest.raises(ValueError, match=r"the set 'big' of Firm joins regions on columns that are not its key \(id\)"):
        load(str(_mapping(tmp_path, _query("BIG", " (set: big)", sql) + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms"))))


def test_a_join_on_the_key_of_what_it_joins_keeps_the_key(tmp_path):
    sql = "SELECT b.id, b.name FROM big_firms b INNER JOIN regions r ON r.id = b.id"
    load(str(_mapping(tmp_path, _query("BIG", " (set: big)", sql) + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms"))))


def test_a_distinct_set_has_every_column_as_its_key_and_a_grouped_set_the_group_by_as_its_key(tmp_path):
    both = _COLUMNS.replace("|     | Y        | legalName", "| PK  | N        | legalName")
    big = _query("BIG", " (set: big)", "SELECT DISTINCT id, name FROM big_firms", columns=both)
    small = _query("SMALL", " (set: small)", "SELECT DISTINCT id, name FROM small_firms", columns=both)
    load(str(_mapping(tmp_path, big + small)))
    grouped = _query("BIG", " (set: big)", "SELECT id, MAX(name) AS name FROM big_firms GROUP BY id")
    load(str(_mapping(tmp_path, grouped + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms"))))  # the group by is the key
    by_name = _query("BIG", " (set: big)", "SELECT name, MAX(id) AS id FROM big_firms GROUP BY name",
                     columns=_COLUMNS)
    with pytest.raises(ValueError, match=r"the set 'big' of Firm is grouped by \['name'\] but its key is \['id'\]"):
        load(str(_mapping(tmp_path, by_name + _query("SMALL", " (set: small)", "SELECT id, name FROM small_firms"))))


# --- written back ------------------------------------------------------------------------------------------------------------------------

def test_the_sets_survive_writing(tmp_path):
    first = load(str(_mapping(tmp_path)))
    path = tmp_path / "written.md"
    from datafinder_examples import example_path
    save(str(path), "Sets", first, [str(example_path(_MODEL))])
    text = path.read_text()
    assert "(set: big)" in text and "(set: small)" in text and "UNION" not in text
    again = _firm(load(str(path)))
    assert [s.set_name for s in again.sets] == ["big", "small"]


# --- an association between classes that have sets says which sets it joins ------------------------------------------------------------------

_PEOPLE_TABLES = """#### Table: people

| Column  | Type | Key | Nullable | Property ID |
|---------|------|-----|----------|-------------|
| id      | INT  | PK  | N        | id          |
| name    | VARCHAR |  | Y        | firstName   |
| age     | INT  |     | Y        | age         |
| firm_id | INT  |     | Y        |             |

"""
_PERSON_COLUMNS = """| Column  | Type    | Key | Nullable | Property ID |
|---------|---------|-----|----------|-------------|
| id      | INT     | PK  | N        | id          |
| name    | VARCHAR |     | Y        | firstName   |
| age     | INT     |     | Y        | age         |
| firm_id | INT     |     | Y        |             |
"""
_PERSON_SETS = (_query("STAFF_Q", " (set: staff)", "SELECT id, name, age, firm_id FROM staff", _PERSON_COLUMNS, "Person")
                + _query("CONTRACTORS_Q", " (set: contractors)", "SELECT id, name, age, firm_id FROM contractors", _PERSON_COLUMNS, "Person"))
_PERSON_TABLES = "".join(f"""#### Table: {t}

| Column  | Type | Key | Nullable | Property ID |
|---------|------|-----|----------|-------------|
| id      | INT  | PK  | N        |             |
| name    | VARCHAR |  | Y        |             |
| age     | INT  |     | Y        |             |
| firm_id | INT  |     | Y        |             |

""" for t in ("staff", "contractors"))


def _assoc(options, source_column="id", target="people", target_column="firm_id"):
    return f"""#### Association: FirmPerson{options}

| Source Column | Target Table | Target Column |
|---------------|--------------|---------------|
| {source_column} | {target} | {target_column} |

"""


def _employees(mapping):
    return next(pm for pm in _firm(mapping).property_mappings if pm.property.id == "employees")


def test_an_association_from_a_class_of_sets_names_the_sets_it_joins_and_the_join_is_on_them(tmp_path):
    from model.relational import ComparisonOperation, LogicalOperation
    mapping = load(str(_mapping(tmp_path, _DEFAULT + _PEOPLE_TABLES.replace("#### Table: people", "#### Table: people → Person") + _assoc(" (source set: big)"))))
    join = _employees(mapping).target
    assert join.set_pairs == [("big", None)]  # type: ignore[attr-defined]
    condition = join.condition  # type: ignore[attr-defined]
    assert isinstance(condition, ComparisonOperation) and condition.left.name == "_set" and condition.right.value == "big"  # type: ignore[attr-defined]
    assert not isinstance(condition, LogicalOperation)


def test_an_association_between_classes_of_sets_pairs_the_sets_and_nothing_else(tmp_path):
    from model.relational import LogicalOperation, LogicalOperator
    text = _DEFAULT + _PERSON_TABLES + _PERSON_SETS + _assoc(" (source set: big, target set: staff)", target="STAFF_Q") + \
        _assoc(" (source set: small, target set: contractors)", target="CONTRACTORS_Q")
    join = _employees(load(str(_mapping(tmp_path, text)))).target
    assert join.set_pairs == [("big", "staff"), ("small", "contractors")]  # type: ignore[attr-defined]
    assert isinstance(join.condition, LogicalOperation) and join.condition.operator == LogicalOperator.OR  # type: ignore[attr-defined]


@pytest.mark.parametrize("assoc,message", [
    (_assoc(""), "Firm has sets: the association FirmPerson must say its source set"),
    (_assoc(" (source set: nope)"), "'nope' is not a set of Firm"),
    (_assoc(" (source set: big, target set: staff)"), "Person has no sets: 'staff' is not one"),
    (_assoc(" (source set: big)") + _assoc(" (source set: big)"), r"the sets \(big\) of FirmPerson are given twice"),
    (_assoc(" (source set: big)") + _assoc(" (source set: small)", source_column="name"), "the sections of FirmPerson join on different columns"),
    (_assoc(" (target set: big)"), "Firm has sets: the association FirmPerson must say its source set"),
])
def test_an_association_of_sets_is_refused_unless_it_names_real_sets_once_each(tmp_path, assoc, message):
    text = _DEFAULT + _PEOPLE_TABLES.replace("#### Table: people", "#### Table: people → Person") + assoc
    with pytest.raises(ValueError, match=message):
        load(str(_mapping(tmp_path, text)))


def test_an_association_that_joins_to_the_set_of_a_class_must_name_the_sets(tmp_path):
    text = _DEFAULT + _PERSON_TABLES + _PERSON_SETS + _assoc(" (source set: big)", target="STAFF_Q")
    with pytest.raises(ValueError, match="Person has sets: the association FirmPerson must say its target set"):
        load(str(_mapping(tmp_path, text)))


def test_the_target_table_of_a_set_is_the_query_of_that_set(tmp_path):
    text = _DEFAULT + _PERSON_TABLES + _PERSON_SETS + _assoc(" (source set: big, target set: staff)", target="CONTRACTORS_Q")
    with pytest.raises(ValueError, match="the target set 'staff' of FirmPerson is the query STAFF_Q, not CONTRACTORS_Q"):
        load(str(_mapping(tmp_path, text)))


def test_the_sets_of_an_association_survive_writing(tmp_path):
    from datafinder_examples import example_path
    text = _DEFAULT + _PERSON_TABLES + _PERSON_SETS + _assoc(" (source set: big, target set: staff)", target="STAFF_Q") + \
        _assoc(" (source set: small, target set: contractors)", target="CONTRACTORS_Q")
    path = tmp_path / "written.md"
    save(str(path), "Sets", load(str(_mapping(tmp_path, text))), [str(example_path(_MODEL))])
    written = path.read_text()
    assert "#### Association: FirmPerson (source set: big, target set: staff)" in written
    assert "#### Association: FirmPerson (source set: small, target set: contractors)" in written
    assert _employees(load(str(path))).target.set_pairs == [("big", "staff"), ("small", "contractors")]  # type: ignore[attr-defined]


def test_the_sets_must_have_the_same_milestoning(tmp_path):
    scheme = """| Scheme              | processing_start | processing_end | business_date | business_date_from | business_date_to | infinite_datetime |
|---------------------|------------------|----------------|---------------|--------------------|------------------|-------------------|
| business_date_range |                  |                |               | from_z             | thru_z           | 9999-12-31        |

"""
    dated = _COLUMNS + "| from_z | DATE    |     | Y        | businessFrom |\n| thru_z | DATE    |     | Y        | businessThru |\n"
    text = _mapping(tmp_path, _query("BIG", " (set: big, milestoning: business_date_range)", "SELECT id, name, from_z, thru_z FROM big_firms", dated)
                    + _query("SMALL", " (set: small)", "SELECT id, name, from_z, thru_z FROM small_firms",
                                      _COLUMNS + "| from_z | DATE    |     | Y        |              |\n| thru_z | DATE    |     | Y        |              |\n"))
    text.write_text(text.read_text().replace("### Schema: org", scheme + "### Schema: org", 1))
    with pytest.raises(ValueError, match="the sets 'big' and 'small' of Firm have different milestoning"):
        load(str(text))
