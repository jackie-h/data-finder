"""A column of a result is named as it was asked to be named, so a name that is not a plain identifier is quoted where it is read."""
from datafinder.sql_generator import _column_reference


def test_a_plain_name_is_not_quoted():
    assert _column_reference("t0", "first_name") == "t0.first_name"


def test_a_name_with_a_space_is_quoted():
    assert _column_reference("t0", "First Name") == 't0."First Name"'


def test_a_quote_in_a_name_is_doubled():
    assert _column_reference("t0", 'say "hi"') == 't0."say ""hi"""'


def test_a_name_that_starts_with_a_digit_is_quoted():
    assert _column_reference("t0", "2nd") == 't0."2nd"'
