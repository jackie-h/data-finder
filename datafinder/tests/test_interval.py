"""The time between two datetimes is an interval, a ``timedelta``: it is ordered as one, its smallest and largest are
intervals, and a column that holds one is read as one by the step after it."""
import datetime

import pytest

from datafinder import DateTimeAttribute, IntegerAttribute, constant
from datafinder.runner import FinderResult
from datafinder.typed_attributes import IntervalAttribute, typed_attribute
from model.relational import (
    IntervalAggregateOperation, IntervalConstantOperation, IntervalOperation, NoOperation, Table,
)

_DAY = datetime.timedelta(days=1)


def _versions() -> tuple[FinderResult, DateTimeAttribute, DateTimeAttribute]:
    table = Table("versions", [("START_TIME", "TIMESTAMP"), ("END_TIME", "TIMESTAMP"), ("QTY", "INT")])
    start = DateTimeAttribute("Start", "START_TIME", "TIMESTAMP", table)
    end = DateTimeAttribute("End", "END_TIME", "TIMESTAMP", table)
    return FinderResult(None, None, [start.alias("start"), end.alias("end")], table, NoOperation()), start, end


def test_a_datetime_minus_a_datetime_is_an_interval():
    _, start, end = _versions()
    assert isinstance(end - start, IntervalOperation)
    assert isinstance(end - datetime.datetime(2005, 1, 1), IntervalOperation)
    assert isinstance(datetime.datetime(2005, 1, 1) - end, IntervalOperation)


def test_a_datetime_minus_a_timedelta_is_still_a_datetime():
    _, start, _ = _versions()
    assert not isinstance(start - _DAY, IntervalOperation)


def test_an_interval_is_written_as_the_difference_of_the_two_times():
    result, start, end = _versions()
    table = result._table
    sql = FinderResult(None, None, [(end - start).alias("length")], table, NoOperation()).to_sql()
    assert sql == 'SELECT t0.END_TIME - t0.START_TIME AS "length" FROM versions AS t0'


def test_a_datetime_constant_in_an_interval_is_typed_as_a_timestamp():
    result, start, end = _versions()
    sql = FinderResult(None, None, [(end - datetime.datetime(2005, 1, 1)).alias("age")], result._table, NoOperation()).to_sql()
    assert "t0.END_TIME - TIMESTAMP '2005-01-01 00:00:00'" in sql


def test_an_interval_is_compared_with_a_timedelta_or_another_interval():
    result, start, end = _versions()
    sql = FinderResult(None, None, [start], result._table, (end - start) > _DAY).to_sql()
    assert "WHERE t0.END_TIME - t0.START_TIME > INTERVAL '86400' SECOND" in sql
    assert "<=" in FinderResult(None, None, [start], result._table, (end - start) <= (end - start)).to_sql()


def test_the_smallest_and_largest_interval_are_intervals():
    _, start, end = _versions()
    assert isinstance((end - start).min(), IntervalAggregateOperation)
    assert isinstance((end - start).max(), IntervalAggregateOperation)


def test_an_interval_has_no_arithmetic_or_sum():
    _, start, end = _versions()
    assert not hasattr(end - start, "sum") and not hasattr(end - start, "average")


def test_an_interval_constant_is_a_timedelta_in_seconds():
    assert isinstance(constant(_DAY), IntervalConstantOperation)
    result, start, _ = _versions()
    sql = FinderResult(None, None, [constant(datetime.timedelta(seconds=90.5)).alias("c")], result._table, NoOperation()).to_sql()
    assert "INTERVAL '90.5' SECOND" in sql


def test_a_column_of_intervals_is_read_as_one_by_the_step_after_it():
    result, start, end = _versions()
    lengths = FinderResult(None, None, [(end - start).alias("length")], result._table, NoOperation())
    assert lengths["length"].column().type == "INTERVAL"
    assert isinstance(lengths["length"], IntervalAttribute)
    assert "WHERE t0.length > INTERVAL '86400' SECOND" in lengths.filter(lambda r: r.length > _DAY).to_sql()


def test_typed_attribute_makes_an_interval_attribute_for_an_interval_column():
    table = Table("t", [("D", "INTERVAL")])
    assert isinstance(typed_attribute("d", "INTERVAL", table), IntervalAttribute)


def test_a_case_of_intervals_takes_the_longer_and_is_typed_as_an_interval():
    from datafinder.conditional import when
    result, start, end = _versions()
    gap = end - start
    longer = when(gap > _DAY).then(gap).otherwise(_DAY)
    step = FinderResult(None, None, [longer.alias("longer")], result._table, NoOperation())
    assert step["longer"].column().type == "INTERVAL"


def test_an_interval_column_comes_back_as_timedeltas():
    import pyarrow as pa
    from datafinder.output import arrow_table_to_numpy, arrow_table_to_pandas
    interval = pa.MonthDayNano
    table = pa.table({"d": pa.array([pa.scalar((0, 2, 5_000_000_000), pa.month_day_nano_interval()).as_py(), None],
                                    pa.month_day_nano_interval())})
    assert interval  # the values are (months, days, nanoseconds)
    assert arrow_table_to_numpy(table)[0, 0] == datetime.timedelta(days=2, seconds=5)
    assert arrow_table_to_pandas(table)["d"][0] == datetime.timedelta(days=2, seconds=5)


def test_an_interval_of_months_has_no_fixed_length_and_is_refused():
    import pyarrow as pa
    from datafinder.output import arrow_table_to_numpy
    table = pa.table({"d": pa.array([(1, 0, 0)], pa.month_day_nano_interval())})
    with pytest.raises(ValueError, match="months"):
        arrow_table_to_numpy(table)
