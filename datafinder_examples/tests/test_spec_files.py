"""The spec files of data-finder-specs: the typed CSV of a result, and that the files are the specs of the Python."""
import datetime
import decimal
import json

import numpy as np
import pytest

from datafinder_examples_tests import spec_files, suite_data, typed_csv


def _round_trip(columns, types, rows):
    columns2, types2, values = typed_csv.read(typed_csv.write(columns, types, rows))
    assert (columns2, types2) == (columns, types)
    return values.tolist()


def test_a_missing_value_and_the_empty_string_are_different():
    rows = [["a", "b"], [None, ""], ["", None]]
    assert _round_trip(["x", "y"], ["String[0..1]", "String[0..1]"], rows) == rows


def test_a_cell_with_a_comma_a_quote_or_a_line_break_is_quoted():
    rows = [['a, "b"'], ["two\nlines"], ["plain"]]
    text = typed_csv.write(["Name"], ["String"], rows)
    assert '"a, ""b"""' in text and "plain\n" in text
    assert _round_trip(["Name"], ["String"], rows) == rows


def test_each_type_comes_back_as_the_value_it_was():
    row = [1, 0.1 + 0.2, decimal.Decimal("1.50"), True, datetime.date(2024, 2, 29), datetime.datetime(2016, 8, 10, 8, 55, 30, 250000), [1.0, 0.2]]
    types = ["Integer", "Double", "Decimal", "Boolean", "Date", "DateTime", "Vector"]
    assert _round_trip([f"c{i}" for i in range(7)], types, [row]) == [row]


def test_an_interval_is_an_iso_duration():
    values = [datetime.timedelta(days=731), datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500000),
              datetime.timedelta(seconds=-90), datetime.timedelta(0)]
    text = typed_csv.write(["d"], ["Interval"], [[v] for v in values])
    assert text.splitlines()[2:] == ["P731D", "P1DT2H3M4.5S", "-P0DT1M30S", "P0D"]
    assert [row[0] for row in _round_trip(["d"], ["Interval"], [[v] for v in values])] == values


def test_the_type_of_a_column_is_worked_out_from_its_values():
    assert typed_csv.column_type([1, 2]) == "Integer"
    assert typed_csv.column_type([1, None]) == "Integer[0..1]"
    assert typed_csv.column_type([1, 2.5]) == "Double"
    assert typed_csv.column_type([np.int64(3), np.nan]) == "Integer[0..1]"
    assert typed_csv.column_type([datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)]) == "DateTime"
    assert typed_csv.column_type([datetime.date(2024, 1, 1)]) == "Date"
    with pytest.raises(ValueError, match="more than one type"):
        typed_csv.column_type([1, "a"])
    with pytest.raises(ValueError, match="no value"):
        typed_csv.column_type([None])


def test_a_row_with_the_wrong_number_of_cells_is_refused():
    with pytest.raises(ValueError, match="has 1 cells"):
        typed_csv.read("a,b\nInteger,Integer\n1\n")


def test_the_query_of_each_spec_file_is_what_the_python_spec_has():
    """Run ``uv run datafinder_examples/tools/write_spec_files.py`` to write the JSON again after a query changes or is added."""
    synced = spec_files.sync_all()
    root = spec_files.specs_folder()
    on_disk = {path: (root / path).read_text(encoding="utf-8") if (root / path).is_file() else None for path in synced}
    assert [path for path in synced if on_disk[path] != synced[path]] == []


def test_each_spec_has_its_files_and_each_file_a_spec():
    """The files of a group are, for each spec of the Python, its JSON, the CSV of its result and the SQL files its JSON names: no other."""
    root = spec_files.specs_folder()
    problems = []
    for suite in spec_files.SUITES:
        expected = set()
        for group, spec in spec_files.groups(suite).items():
            for expectation in spec.expectations:
                folder = f"{suite.name}/{group}"
                document = json.loads((root / folder / f"{expectation.name}.json").read_text(encoding="utf-8"))
                expected |= {f"{folder}/{expectation.name}.json", f"{folder}/{document['result']}",
                             *(f"{folder}/{file}" for file in document.get("sql", {}).values())}
        found = spec_files.files_of(suite)
        problems += [f"missing {path}" for path in sorted(expected - found) if not (root / path).is_file()]
        problems += [f"no spec for {path}" for path in sorted(found - expected)]
    assert problems == []


def test_every_table_of_a_suite_has_a_data_file():
    root = spec_files.specs_folder()
    missing = [f"{suite.name}: {csv}" for suite in spec_files.SUITES for csv in suite.data.values() if not (root / "data" / csv).is_file()]
    assert missing == []


def test_a_suite_creates_every_table_it_loads_and_nothing_else_is_loaded():
    for suite in spec_files.SUITES:
        created = {t.name.removeprefix("main.") for t in suite_data.tables(suite.name)}
        assert set(suite.data) <= created, f"{suite.name} loads a table that schema.sql does not create"


def test_the_tables_of_a_suite_are_read_from_its_schema():
    tables = {t.name: t for t in suite_data.tables("companies")}
    assert tables["main.companies"].columns == "id INT, name VARCHAR, category VARCHAR, embedding VECTOR(3)"
    assert tables["main.companies"].csv == "companies.csv"
    finance = {t.name: t.csv for t in suite_data.tables("finance_group_by")}
    assert finance["ref_data.price"] is None and finance["trading.trades"] == "finance_trades_group_by.csv"
    assert suite_data.statements("finance_group_by")[0] == "DROP SCHEMA IF EXISTS trading CASCADE"
