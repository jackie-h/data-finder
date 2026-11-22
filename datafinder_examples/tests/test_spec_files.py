"""The spec files of data-finder-specs: the typed CSV of a result, and that the files are what the Python specs export."""
import datetime
import decimal
import math

import numpy as np
import pytest

from datafinder_examples_tests import spec_files, typed_csv


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


def _same_file(path: str, written: str, exported: str) -> bool:
    """A file is what the specs export: the same text, and for a result the same values, a double to a relative 1e-9, because the
    last digits of a sine or a mean depend on the maths library of the machine that wrote the file."""
    if written == exported:
        return True
    if not path.endswith(".csv"):
        return False
    columns, types, rows = typed_csv.read(written)
    columns2, types2, rows2 = typed_csv.read(exported)
    if (columns, types, rows.shape) != (columns2, types2, rows2.shape):
        return False
    return all(a == b or (isinstance(a, float) and isinstance(b, float) and math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9))
               for a, b in zip(rows.ravel(), rows2.ravel()))


def test_the_spec_files_are_what_the_python_specs_export():
    """Run ``uv run datafinder_examples/tools/write_spec_files.py`` to write them again after a spec changes."""
    exported = spec_files.export_all()
    root = spec_files.specs_folder()
    on_disk = {p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
               for suite in spec_files.SUITES for p in (root / suite.name).rglob("*") if p.is_file()}
    assert sorted(on_disk) == sorted(exported)
    assert [path for path in exported if not _same_file(path, on_disk[path], exported[path])] == []


def test_a_double_that_differs_in_the_last_digits_is_the_same_file():
    one = typed_csv.write(["x"], ["Double"], [[0.1 + 0.2]])
    other = typed_csv.write(["x"], ["Double"], [[0.30000000000000004 + 1e-16]])
    assert _same_file("a.csv", one, other)
    assert not _same_file("a.csv", one, typed_csv.write(["x"], ["Double"], [[0.31]]))
    assert not _same_file("a.json", one, other)


def test_every_table_of_a_suite_has_a_data_file():
    root = spec_files.specs_folder()
    missing = [f"{suite.name}: {csv}" for suite in spec_files.SUITES for csv in suite.data.values() if not (root / "data" / csv).is_file()]
    assert missing == []
