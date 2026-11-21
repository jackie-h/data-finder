"""The SQL the generator writes is Postgres SQL, which DuckDB runs once sqlglot has translated it, and it gives what DuckDB's own functions gave."""
import datetime
import decimal

import duckdb
import pytest

from datafinder import sql_for_dialect
from model.relational import DatePart
from datafinder.sql_generator import _date_diff_string, _interval_string

_DATES = ["2023-12-31", "2024-01-01", "2024-02-28", "2024-02-29", "2024-03-01", "2024-03-31", "2024-04-01", "2024-06-15", "2024-12-31", "2025-01-01",
          "2025-01-05", "2025-01-06", "2025-01-12"]
_TIMES = ["2024-03-10 08:59:59", "2024-03-10 09:00:00", "2024-03-10 09:01:00", "2024-03-10 09:59:59.9", "2024-03-10 10:00:00", "2024-03-11 08:59:59",
          "2024-03-11 09:00:00", "2023-12-31 23:59:59", "2024-01-01 00:00:00", "2024-01-01 00:00:01"]


@pytest.fixture(scope="module")
def con():
    return duckdb.connect()


def _run(con, sql: str):
    return con.sql(sql_for_dialect(sql, "duckdb")).fetchone()[0]


@pytest.mark.parametrize("unit", [DatePart.YEAR, DatePart.QUARTER, DatePart.MONTH, DatePart.WEEK, DatePart.DAY])
def test_a_difference_of_dates_counts_what_duckdbs_date_diff_counts(con, unit):
    for a in _DATES:
        for b in _DATES:
            ours = _run(con, "SELECT " + _date_diff_string(unit, f"CAST('{a}' AS DATE)", f"CAST('{b}' AS DATE)"))
            assert ours == con.sql(f"SELECT DATE_DIFF('{unit.value.lower()}', DATE '{a}', DATE '{b}')").fetchone()[0], (unit, a, b)


@pytest.mark.parametrize("unit", [DatePart.HOUR, DatePart.MINUTE, DatePart.SECOND])
def test_a_difference_of_times_counts_what_duckdbs_date_diff_counts(con, unit):
    for a in _TIMES:
        for b in _TIMES:
            ours = _run(con, "SELECT " + _date_diff_string(unit, f"CAST('{a}' AS TIMESTAMP)", f"CAST('{b}' AS TIMESTAMP)"))
            assert ours == con.sql(f"SELECT DATE_DIFF('{unit.value.lower()}', TIMESTAMP '{a}', TIMESTAMP '{b}')").fetchone()[0], (unit, a, b)


def test_a_difference_in_microseconds_is_refused():
    with pytest.raises(ValueError, match="not MICROSECOND"):
        _date_diff_string(DatePart.MICROSECOND, "a", "b")


@pytest.mark.parametrize("n", [1, 5, 300, -7])
@pytest.mark.parametrize("unit, native", [(DatePart.WEEK, "WEEK"), (DatePart.QUARTER, "QUARTER"), (DatePart.MICROSECOND, "MICROSECOND"),
                                           (DatePart.DAY, "DAY"), (DatePart.MONTH, "MONTH"), (DatePart.SECOND, "SECOND")])
def test_an_interval_in_the_units_ansi_has_is_the_length_of_time_it_names(con, n, unit, native):
    ours = _run(con, f"SELECT TIMESTAMP '2024-03-10 08:00:00' + {_interval_string(n, unit)}")
    assert ours == con.sql(f"SELECT TIMESTAMP '2024-03-10 08:00:00' + {n} * INTERVAL 1 {native}").fetchone()[0], (n, unit)


def test_only_the_units_ansi_has_are_written_in_an_interval():
    assert _interval_string(2, DatePart.WEEK) == "INTERVAL '14' DAY"
    assert _interval_string(1, DatePart.QUARTER) == "INTERVAL '3' MONTH"
    assert _interval_string(5, DatePart.MICROSECOND) == "INTERVAL '0.000005' SECOND"
    assert _interval_string(-1_500_000, DatePart.MICROSECOND) == "INTERVAL '-1.500000' SECOND"
    assert _interval_string(3, DatePart.HOUR) == "INTERVAL '3' HOUR"
    with pytest.raises(ValueError, match="not counted in DOW"):
        _interval_string(1, DatePart.DOW)


def test_the_name_of_a_day_is_what_duckdbs_dayname_gives(con):
    from datafinder.sql_generator import _DAY_NAMES
    day = datetime.date(2024, 3, 4)  # a Monday
    for offset in range(14):
        d = day + datetime.timedelta(days=offset)
        names = " ".join(f"WHEN {number} THEN '{name}'" for number, name in enumerate(_DAY_NAMES, 1))
        ours = _run(con, f"SELECT CASE EXTRACT(ISODOW FROM CAST('{d}' AS DATE)) {names} END")
        assert ours == con.sql(f"SELECT DAYNAME(DATE '{d}')").fetchone()[0], d


def test_a_regular_expression_matches_anywhere_in_the_text_as_it_did(con):
    assert _run(con, "SELECT 'Customer X Complaints' ~ 'X.*Compl'") is True
    assert _run(con, "SELECT 'abc' ~ '^b'") is False
    assert _run(con, "SELECT 'abc' ~ 'b'") is True  # DuckDB's own ~ would have to match the whole text
