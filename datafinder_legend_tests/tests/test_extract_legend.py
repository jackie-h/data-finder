"""The extractor names each Legend test uniquely within its file, since a test is identified by file and name."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from extract_legend import extract_file  # noqa: E402

_PURE = """
function <<test.Test>> a::b::rtrim::testProject():Boolean[1]
{
  let result = execute(|T.all()->project([s | $s.x], ['x']), m, r, e);
  assertEquals('select 1 as "x"', $result->sqlRemoveFormatting());
}

function <<test.Test>> a::b::left::testProject():Boolean[1]
{
  let result = execute(|T.all()->project([s | $s.y], ['y']), m, r, e);
  assertEquals('select 2 as "y"', $result->sqlRemoveFormatting());
}

function <<test.Test>> a::b::left::testOnly():Boolean[1]
{
  let result = execute(|T.all()->project([s | $s.y], ['y']), m, r, e);
  assertEquals('select 3 as "y"', $result->sqlRemoveFormatting());
}
"""


def _tests(tmp_path):
    path = tmp_path / "t.pure"
    path.write_text(_PURE)
    return {t["name"]: t for t in extract_file(path, tmp_path)}


def test_names_repeated_in_a_file_are_qualified_by_their_package(tmp_path):
    names = _tests(tmp_path)
    assert {"rtrim::testProject", "left::testProject"} <= set(names)
    assert "testProject" not in names


def test_each_qualified_test_keeps_its_own_sql(tmp_path):
    names = _tests(tmp_path)
    assert names["rtrim::testProject"]["expected_sql"][0]["sql"] == ['select 1 as "x"']
    assert names["left::testProject"]["expected_sql"][0]["sql"] == ['select 2 as "y"']


def test_a_name_that_is_not_repeated_stays_the_function_name(tmp_path):
    assert "testOnly" in _tests(tmp_path)


def test_a_repeated_name_is_left_alone_when_only_one_of_them_is_a_projection_test(tmp_path):
    path = tmp_path / "t.pure"
    path.write_text("""
function <<test.Test>> a::query::testSame():Boolean[1]
{
  let result = execute(|T.all()->filter(s | $s.x == 1), m, r, e);
  assertEquals(1, $result.values->size());
}

function <<test.Test>> a::projection::testSame():Boolean[1]
{
  let result = execute(|T.all()->project([s | $s.x], ['x']), m, r, e);
  assertEquals('select 1 as "x"', $result->sqlRemoveFormatting());
}
""")
    projection = [t for t in extract_file(path, tmp_path) if t["kind"] == "project"]
    assert [t["name"] for t in projection] == ["testSame"]
