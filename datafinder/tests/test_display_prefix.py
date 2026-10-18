"""Display names chain along the join path from the root, so a column's name says how it was reached."""
from datafinder.finder import display_prefix
from model.relational import Column, JoinOperation, JoinTreeNodeOperation, Table


def _node(name, parent=None):
    join = JoinOperation(name, Table(name.lower(), []), Column("fk", "INT", "t"), Column("id", "INT", name.lower()))
    return JoinTreeNodeOperation(join, parent)


def test_a_single_hop_is_just_its_relation_name():
    assert display_prefix("Account", None) == "Account"


def test_each_hop_is_prefixed_by_the_hops_before_it():
    account = _node("Account")
    branch = _node("Branch", account)
    assert display_prefix("Branch", account) == "Account Branch"
    assert display_prefix("City", branch) == "Account Branch City"


def test_names_are_title_cased_per_hop():
    assert display_prefix("project", _node("manager")) == "Manager Project"
