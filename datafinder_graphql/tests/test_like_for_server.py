"""A LIKE pattern built from a value uses ! to make the next character literal; the server's escape is a backslash."""
import pytest

from datafinder_graphql.graphql_engine import _like_for_server


@pytest.mark.parametrize("pattern,expected", [
    ("%abc%", "%abc%"),
    ("%50!%%", "%50\\%%"),
    ("a!_b%", "a\\_b%"),
    ("wow!!%", "wow!%"),
    ("back\\slash%", "back\\\\slash%"),
    ("trailing!", "trailing!"),
])
def test_conversion(pattern, expected):
    assert _like_for_server(pattern) == expected
