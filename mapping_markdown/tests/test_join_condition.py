"""A join condition is read as SQL over the columns of the two tables, and what it cannot read is refused."""
import pytest

from mapping_markdown.join_condition import parse_condition
from model.relational import (
    ComparisonOperation, ComparisonOperator, IntegerConstantOperation, IsNotNullOperation, IsNullOperation,
    LogicalOperation, LogicalOperator, StringConstantOperation, Table,
)

_FIRM = Table("firmTable", [("ID", "INT"), ("FLAG", "INT")])
_PERSON = Table("personTable", [("ID", "INT"), ("FIRSTNAME", "VARCHAR"), ("FLAG", "INT"), ("MANAGERID", "INT")])


def _parse(text):
    return parse_condition(text, [_FIRM, _PERSON], "Association 'FirmPerson'")


def test_is_null_and_is_not_null_of_each_table():
    op = _parse("firmTable.FLAG IS NULL AND personTable.FLAG IS NOT NULL")
    assert isinstance(op, LogicalOperation) and op.operator == LogicalOperator.AND
    assert isinstance(op.left, IsNullOperation) and op.left.element.owner_relation is _FIRM  # type: ignore[attr-defined]
    assert isinstance(op.right, IsNotNullOperation) and op.right.element.owner_relation is _PERSON  # type: ignore[attr-defined]


def test_an_or_group_of_comparisons_with_string_literals():
    op = _parse("(personTable.FIRSTNAME = 'Peter' OR personTable.FIRSTNAME = 'John')")
    assert isinstance(op, LogicalOperation) and op.operator == LogicalOperator.OR
    assert isinstance(op.left, ComparisonOperation) and op.left.operator == ComparisonOperator.EQUAL
    assert isinstance(op.left.right, StringConstantOperation) and op.left.right.value == "Peter"


@pytest.mark.parametrize("text", ["personTable.ID <> -99999999", "personTable.ID != -99999999"])
def test_not_equal_and_a_negative_number(text):
    op = _parse(text)
    assert isinstance(op, ComparisonOperation) and op.operator == ComparisonOperator.NOT_EQUAL
    assert isinstance(op.right, IntegerConstantOperation) and op.right.value == -99999999


def test_a_schema_qualified_column():
    table = Table("hr.personTable", [("ID", "INT")])
    op = parse_condition("hr.personTable.ID = 1", [table], "J")
    assert isinstance(op, ComparisonOperation) and op.left.owner_relation is table  # type: ignore[attr-defined]


def test_target_in_a_join_of_a_table_to_itself_is_an_alias_of_the_table():
    from model.relational import TableAlias
    op = parse_condition("personTable.MANAGERID = {target}.ID AND {target}.FLAG IS NULL", [_PERSON, _PERSON], "J")
    assert isinstance(op, LogicalOperation)
    assert op.left.left.owner_relation is _PERSON  # type: ignore[attr-defined]  # the side the join starts from
    alias = op.left.right.owner_relation  # type: ignore[attr-defined]
    assert isinstance(alias, TableAlias) and alias.relation is _PERSON
    assert op.right.element.owner_relation is alias  # type: ignore[attr-defined]  # one alias for every {target}


def test_target_in_a_join_to_another_table_is_that_table():
    op = parse_condition("{target}.FLAG IS NULL AND personTable.FLAG IS NOT NULL", [_PERSON, _FIRM], "J")
    assert isinstance(op, LogicalOperation)
    assert op.left.element.owner_relation is _FIRM  # type: ignore[attr-defined]


@pytest.mark.parametrize("text,message", [
    ("{target}.NOPE = 1", "has no column"),
    ("FLAG IS NULL", "needs its table"),
    ("otherTable.X = 1", "not one of the two tables"),
    ("personTable.NOPE = 1", "has no column"),
    ("now() < personTable.ID", "a function in a join condition is not supported"),
    ("personTable.ID IN (1, 2)", "not a condition that can be read"),
    ("personTable.ID = firmTable.ID +", "cannot read the condition"),
])
def test_what_cannot_be_read_is_refused_and_says_why(text, message):
    with pytest.raises(ValueError, match=message):
        parse_condition(text, [_FIRM, _PERSON], "Association 'FirmPerson'")


class TestConditionsInAMapping:
    """An Association table's Condition column reaches the join, and is written back as it was."""

    def _load(self, path=None):
        from datafinder_examples import example_path
        from mapping_markdown.markdown_mapping import load
        return load(str(path or example_path("join_conditions_mapping.md")))

    def _join(self, mapping, class_name, property_id):
        from model.relational_mapping import Join
        rcm = next(r for r in mapping.mappings if r.clazz.name == class_name)
        join = next(pm.target for pm in rcm.property_mappings if pm.property.id == property_id)
        assert isinstance(join, Join)
        return join

    def test_a_condition_reaches_the_join_of_its_hop(self):
        mapping = self._load()
        firm = self._join(mapping, "Person", "firm")
        assert firm.via == [] and firm.condition is not None
        assert firm.condition_texts == ["firms.flag IS NULL AND persons.flag IS NOT NULL"]

    def test_each_hop_of_a_path_has_its_own_condition(self):
        join = self._join(self._load(), "Person", "activeFirms")
        assert len(join.via) == 1 and join.via_conditions == [None]
        assert join.condition_texts == [None, "memberships.active = 1"]

    def test_the_conditions_survive_writing(self, tmp_path):
        from datafinder_examples import example_path
        from mapping_markdown.markdown_mapping import save
        mapping = self._load()
        path = tmp_path / "mapping.md"
        save(str(path), "Join Conditions Mapping", mapping, [str(example_path("join_conditions_model.md"))])
        again = self._load(path)
        assert self._join(again, "Person", "firm").condition_texts == ["firms.flag IS NULL AND persons.flag IS NOT NULL"]
        assert self._join(again, "Person", "activeFirms").condition_texts == [None, "memberships.active = 1"]

    def test_a_condition_on_a_table_joined_to_itself_survives_writing(self, tmp_path):
        from datafinder_examples import example_path
        from mapping_markdown.markdown_mapping import save
        mapping = self._load()
        manager = self._join(mapping, "Employee", "manager")
        assert manager.condition_texts == ["staff.active = 1 AND {target}.active = 1"]
        path = tmp_path / "mapping.md"
        save(str(path), "Join Conditions Mapping", mapping, [str(example_path("join_conditions_model.md"))])
        assert self._join(self._load(path), "Employee", "manager").condition_texts == manager.condition_texts

    def test_a_table_with_no_condition_writes_no_condition_column(self, tmp_path):
        from datafinder_examples import example_path
        from mapping_markdown.markdown_mapping import save
        mapping = self._load(example_path("joins_through_tables_mapping.md"))
        path = tmp_path / "mapping.md"
        save(str(path), "Joins Through Tables Mapping", mapping, [str(example_path("joins_through_tables_model.md"))])
        assert "Condition" not in path.read_text()
