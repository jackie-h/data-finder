"""The rows a Legend test asserts are read from its Pure source, and compared with ours."""
import datetime
import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from extract_rows import Unsupported, asserted, pure_value  # noqa: E402

from datafinder_legend_tests.asserted import check, same


def test_a_pure_literal_is_read():
    assert pure_value("['a', 1, 2.5, true, ^TDSNull()]") == ["a", 1, 2.5, True, None]
    assert pure_value("'name,code\\n' + 'IF 1,1001\\n'") == "name,code\nIF 1,1001\n"
    assert pure_value("[%2015-8-15T00:00:00.000000000+0000]") == [{"d": "2015-08-15T00:00:00"}]
    assert pure_value("%2014-12-04T15:22:23.123456") == {"d": "2014-12-04T15:22:23.123456"}
    with pytest.raises(Unsupported):
        pure_value("$x")


def _asserted(body: str) -> dict:
    result = asserted(body)
    assert result is not None, "the body asserts something about its rows"
    return result


def _body(*asserts: str) -> str:
    return "let result = execute(|Person.all()->project(...), m, r, e);\n" + "\n".join(asserts)


def test_each_row_by_position_is_the_rows_when_the_count_is_asserted_too():
    claims = _asserted(_body("assertEquals(['A', 1], $tds.rows->at(0).values);", "assertEquals(['B', 2], $tds.rows->at(1).values);",
                            "assertSize($tds.rows, 2);"))["claims"]
    assert {"kind": "rows", "rows": [["A", 1], ["B", 2]]} in claims and {"kind": "count", "n": 2} in claims


def test_some_rows_by_position_are_only_a_subset_of_the_result():
    assert _asserted(_body("assertEquals(['A', 1], $tds.rows->at(0).values);"))["claims"] == [{"kind": "subset", "rows": [["A", 1]]}]


def test_rows_joined_with_make_string_keep_their_separator():
    claim = _asserted(_body("assertEquals('a|1,b|2', $result.values.rows->map(r|$r.values->makeString('|'))->makeString(','));"))["claims"][0]
    assert claim == {"kind": "rows", "rows": ["a|1", "b|2"], "sep": "|"}


def test_a_csv_is_its_rows_without_the_header():
    claim = _asserted(_body("assertEquals('x,y\\n' + 'a,1\\n' + 'b,\\n', $result.values->toOne()->toCSV());"))["claims"][0]
    assert claim["rows"] == [["a", "1"], ["b", ""]] and claim["header"] == ["x", "y"]


def test_assert_same_elements_of_the_values_is_a_bag_of_cells():
    claim = _asserted(_body("assertSameElements(['a', 1, 'b', 2], $result.values.rows.values);"))["claims"][0]
    assert claim == {"kind": "flat", "values": ["a", 1, "b", 2], "sorted": True}


def test_only_the_first_query_is_asserted():
    body = _body("assertEquals(['A'], $r.values.rows.getString('n'));", "let again = execute(|x, m, r, e);",
                 "assertEquals(['Z'], $again.values.rows.getString('n'));")
    assert _asserted(body)["claims"] == [{"kind": "column", "values": ["A"]}]


def test_what_is_not_a_shape_that_is_read_is_left_out():
    assert asserted(_body("assertEquals([['a', 'b']], $result.values.rows.values);")) is None


def test_values_are_the_same_across_spellings():
    assert same(Decimal("1.0"), "1.0") and same(5, 5.0) and same(1.0000000001, 1.0)
    assert same(None, "TDSNull") and same(None, None) and same(None, "")
    assert same(True, "true") and not same(1, True)
    assert same(datetime.datetime(2015, 8, 15), {"d": "2015-08-15T00:00:00"}) and same(datetime.datetime(2015, 8, 15), {"d": "2015-08-15"})
    assert not same(datetime.datetime(2015, 8, 16), {"d": "2015-08-15"})
    assert not same("a", "b") and not same(None, "a")


def test_ours_are_checked_against_the_claims(monkeypatch):
    from datafinder_legend_tests import asserted as module
    monkeypatch.setattr(module, "claims_of", lambda source: [
        {"kind": "rows", "rows": [["a", "1"], ["b", "2"]]}, {"kind": "count", "n": 2}, {"kind": "column", "values": ["a", "b"]}])
    check("t", [("a", 1), ("b", 2)], 2)
    with pytest.raises(AssertionError):
        check("t", [("a", 1), ("c", 2)], 2)
    with pytest.raises(AssertionError):
        check("t", [("a", 1)], 2)
