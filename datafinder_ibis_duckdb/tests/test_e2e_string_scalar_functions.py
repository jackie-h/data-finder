import os
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "companies_mapping.md")
)

_FINDER_MODULES = ["company_finder"]

_COMPANIES = [
    (1, "  Acme Corp  ", "Technology"),
    (2, "beta corp",     "Manufacturing"),
    (3, "Hello World",   "Finance"),
]


def _build_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS companies")
    conn.execute("CREATE TABLE companies (id INT, name VARCHAR, category VARCHAR)")
    for row in _COMPANIES:
        conn.execute("INSERT INTO companies VALUES (?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def CompanyFinder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_MAPPING_FILE)
    generate(mapping, temp_dir)
    _build_test_db()

    from company_finder import CompanyFinder as CF
    yield CF

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestUpper:

    def test_upper_converts_to_uppercase(self, CompanyFinder):
        name = "beta corp"
        result = CompanyFinder.find_all(
            [CompanyFinder.name().upper()],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Upper Name"] == name.upper()

    def test_upper_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().upper()],
        ).to_pandas()
        assert "Upper Name" in result.columns


class TestLower:

    def test_lower_converts_to_lowercase(self, CompanyFinder):
        name = "Hello World"
        result = CompanyFinder.find_all(
            [CompanyFinder.name().lower()],
            CompanyFinder.id_().eq(3),
        ).to_pandas()
        assert result.iloc[0]["Lower Name"] == name.lower()

    def test_lower_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().lower()],
        ).to_pandas()
        assert "Lower Name" in result.columns


class TestStrip:

    def test_strip_removes_surrounding_whitespace(self, CompanyFinder):
        name = "  Acme Corp  "
        result = CompanyFinder.find_all(
            [CompanyFinder.name().strip()],
            CompanyFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Strip Name"] == name.strip()

    def test_strip_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().strip()],
        ).to_pandas()
        assert "Strip Name" in result.columns


class TestLstrip:

    def test_lstrip_removes_leading_whitespace(self, CompanyFinder):
        name = "  Acme Corp  "
        result = CompanyFinder.find_all(
            [CompanyFinder.name().lstrip()],
            CompanyFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Lstrip Name"] == name.lstrip()

    def test_lstrip_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().lstrip()],
        ).to_pandas()
        assert "Lstrip Name" in result.columns


class TestRstrip:

    def test_rstrip_removes_trailing_whitespace(self, CompanyFinder):
        name = "  Acme Corp  "
        result = CompanyFinder.find_all(
            [CompanyFinder.name().rstrip()],
            CompanyFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Rstrip Name"] == name.rstrip()

    def test_rstrip_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().rstrip()],
        ).to_pandas()
        assert "Rstrip Name" in result.columns


class TestLength:

    def test_length_returns_character_count(self, CompanyFinder):
        name = "beta corp"
        result = CompanyFinder.find_all(
            [CompanyFinder.name().length()],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Length Name"] == len(name)

    def test_length_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().length()],
        ).to_pandas()
        assert "Length Name" in result.columns


class TestReverse:

    def test_reverse_reverses_string(self, CompanyFinder):
        name = "beta corp"
        result = CompanyFinder.find_all(
            [CompanyFinder.name().reverse()],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Reverse Name"] == name[::-1]

    def test_reverse_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().reverse()],
        ).to_pandas()
        assert "Reverse Name" in result.columns


class TestLeft:

    def test_left_returns_first_n_chars(self, CompanyFinder):
        name = "beta corp"
        n = 4
        result = CompanyFinder.find_all(
            [CompanyFinder.name().left(n)],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Left Name"] == name[:n]

    def test_left_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().left(3)],
        ).to_pandas()
        assert "Left Name" in result.columns


class TestRight:

    def test_right_returns_last_n_chars(self, CompanyFinder):
        name = "beta corp"
        n = 4
        result = CompanyFinder.find_all(
            [CompanyFinder.name().right(n)],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Right Name"] == name[-n:]

    def test_right_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().right(3)],
        ).to_pandas()
        assert "Right Name" in result.columns


class TestRepeat:

    def test_repeat_duplicates_string(self, CompanyFinder):
        category = "Technology"
        n = 2
        result = CompanyFinder.find_all(
            [CompanyFinder.category().repeat(n)],
            CompanyFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Repeat Category"] == category * n

    def test_repeat_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().repeat(1)],
        ).to_pandas()
        assert "Repeat Name" in result.columns

    def test_mul_operator_matches_repeat(self, CompanyFinder):
        category = "Technology"
        n = 2
        result = CompanyFinder.find_all(
            [CompanyFinder.category() * n],
            CompanyFinder.id_().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Repeat Category"] == category * n


class TestReplace:

    def test_replace_substitutes_substring(self, CompanyFinder):
        name = "beta corp"
        old, new = "corp", "inc"
        result = CompanyFinder.find_all(
            [CompanyFinder.name().replace(old, new)],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Replace Name"] == name.replace(old, new)

    def test_replace_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().replace("a", "b")],
        ).to_pandas()
        assert "Replace Name" in result.columns


class TestSubstring:

    def test_substring_with_start_only(self, CompanyFinder):
        name = "Hello World"
        start = 6
        result = CompanyFinder.find_all(
            [CompanyFinder.name().substring(start)],
            CompanyFinder.id_().eq(3),
        ).to_pandas()
        assert result.iloc[0]["Substring Name"] == name[start:]

    def test_substring_with_start_and_length(self, CompanyFinder):
        name = "Hello World"
        start, length = 0, 5
        result = CompanyFinder.find_all(
            [CompanyFinder.name().substring(start, length)],
            CompanyFinder.id_().eq(3),
        ).to_pandas()
        assert result.iloc[0]["Substring Name"] == name[start:start + length]

    def test_substring_column_name(self, CompanyFinder):
        result = CompanyFinder.find_all(
            [CompanyFinder.name().substring(0)],
        ).to_pandas()
        assert "Substring Name" in result.columns


class TestSlice:

    def test_stop_only_matches_left(self, CompanyFinder):
        name = "beta corp"
        n = 4
        result = CompanyFinder.find_all(
            [CompanyFinder.name()[:n]],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Left Name"] == name[:n]

    def test_negative_start_matches_right(self, CompanyFinder):
        name = "beta corp"
        n = 4
        result = CompanyFinder.find_all(
            [CompanyFinder.name()[-n:]],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Right Name"] == name[-n:]

    def test_reverse_slice_matches_reverse(self, CompanyFinder):
        name = "beta corp"
        result = CompanyFinder.find_all(
            [CompanyFinder.name()[::-1]],
            CompanyFinder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Reverse Name"] == name[::-1]

    def test_start_only_matches_substring(self, CompanyFinder):
        name = "Hello World"
        start = 6
        result = CompanyFinder.find_all(
            [CompanyFinder.name()[start:]],
            CompanyFinder.id_().eq(3),
        ).to_pandas()
        assert result.iloc[0]["Substring Name"] == name[start:]

    def test_start_and_stop_matches_substring(self, CompanyFinder):
        name = "Hello World"
        result = CompanyFinder.find_all(
            [CompanyFinder.name()[0:5]],
            CompanyFinder.id_().eq(3),
        ).to_pandas()
        assert result.iloc[0]["Substring Name"] == name[0:5]
