"""The GraphQL engine sorts frames the way a database does: a missing value where the sort puts it, for each column."""
import pandas as pd

from datafinder import StringAttribute
from datafinder_graphql.graphql_engine import _sort_frame
from model.relational import Table

_TABLE = Table("t", [("a", "VARCHAR")])
_A = StringAttribute("A", "a", "VARCHAR", _TABLE)


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"a": ["b", None, "a", None], "n": [1, 2, 3, 4]})


def test_a_missing_value_is_first_when_ascending_by_default():
    assert list(_sort_frame(_frame(), ["a"], [_A.ascending()])["n"]) == [2, 4, 3, 1]


def test_a_missing_value_is_last_when_descending_by_default():
    assert list(_sort_frame(_frame(), ["a"], [_A.descending()])["n"]) == [1, 3, 2, 4]


def test_a_sort_can_put_a_missing_value_the_other_way():
    assert list(_sort_frame(_frame(), ["a"], [_A.ascending(nulls="last")])["n"]) == [3, 1, 2, 4]
    assert list(_sort_frame(_frame(), ["a"], [_A.descending(nulls="first")])["n"]) == [2, 4, 1, 3]


def test_each_column_puts_a_missing_value_where_its_own_sort_does():
    df = pd.DataFrame({"a": ["x", "x", "y", "y"], "b": [None, 2.0, None, 1.0], "n": [1, 2, 3, 4]})
    ordered = _sort_frame(df, ["a", "b"], [_A.ascending(), _A.descending(nulls="first")])
    assert list(ordered["n"]) == [1, 2, 3, 4]
    ordered = _sort_frame(df, ["a", "b"], [_A.ascending(), _A.descending()])
    assert list(ordered["n"]) == [2, 1, 4, 3]
