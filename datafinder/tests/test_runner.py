import datetime

import numpy as np
import pandas as pd
import pytest

from datafinder import DataFrame, QueryRunnerBase
from datafinder.runner import FinderResult, ViewResult, _DEFAULT_TIMEOUT_MS, convert_date_time, \
    convert_inputs_and_select, convert_inputs_and_select_for_date_range
from datafinder import IntegerAttribute, StringAttribute
from model.relational import Alias, Column, ColumnWithJoin, JoinOperation, JoinTreeNodeOperation, NoOperation, SortDirection, Table
from model.view import View


def _make_result() -> FinderResult:
    return FinderResult(None, None, [], Table("trades", []), NoOperation())


class TestFinderResultTimeout:

    def test_default_timeout_is_sixty_seconds_in_ms(self):
        result = _make_result()
        assert result._timeout_ms == 60_000

    def test_default_matches_module_constant(self):
        assert _DEFAULT_TIMEOUT_MS == 60_000

    def test_timeout_method_sets_value(self):
        result = _make_result().timeout(1000)
        assert result._timeout_ms == 1000

    def test_timeout_method_returns_self_for_chaining(self):
        result = _make_result()
        assert result.timeout(500) is result

    def test_timeout_accepts_sub_second_values(self):
        result = _make_result().timeout(100)
        assert result._timeout_ms == 100


class TestFindForDateRangeValidation:

    def _table(self):
        return Table("trades", [])

    _PDT = datetime.datetime(2024, 6, 1, 0, 0, 0)

    def test_none_business_date_from_raises(self):
        with pytest.raises(ValueError, match="business_date_from"):
            convert_inputs_and_select_for_date_range(
                None, datetime.date(2024, 12, 31), self._PDT, [], self._table(), NoOperation())  # type: ignore[arg-type]

    def test_none_business_date_to_raises(self):
        with pytest.raises(ValueError, match="business_date_to"):
            convert_inputs_and_select_for_date_range(
                datetime.date(2024, 1, 1), None, self._PDT, [], self._table(), NoOperation())  # type: ignore[arg-type]

    def test_both_none_raises_on_from(self):
        with pytest.raises(ValueError, match="business_date_from"):
            convert_inputs_and_select_for_date_range(
                None, None, self._PDT, [], self._table(), NoOperation())  # type: ignore[arg-type]

    def test_none_processing_datetime_raises(self):
        with pytest.raises(ValueError, match="processing_valid_at"):
            convert_inputs_and_select_for_date_range(
                datetime.date(2024, 1, 1), datetime.date(2024, 12, 31),
                None, [], self._table(), NoOperation())  # type: ignore[arg-type]

    def test_valid_dates_returns_finder_result(self):
        result = convert_inputs_and_select_for_date_range(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31),
            self._PDT, [], self._table(), NoOperation())
        assert isinstance(result, FinderResult)
        assert result._business_date == datetime.date(2024, 1, 1)
        assert result._business_date_to == datetime.date(2024, 12, 31)

    def test_string_dates_are_converted(self):
        result = convert_inputs_and_select_for_date_range(
            "2024-01-01", "2024-12-31", "2024-06-01 00:00:00", [], self._table(), NoOperation())
        assert result._business_date == datetime.date(2024, 1, 1)
        assert result._business_date_to == datetime.date(2024, 12, 31)


class TestConvertDateTime:
    """A processing date input keeps the precision it was given: a date stays a date, a datetime a datetime."""

    def test_datetime_string_is_a_datetime(self):
        result = convert_date_time("2024-06-01 10:30:00")
        assert result == datetime.datetime(2024, 6, 1, 10, 30)
        assert isinstance(result, datetime.datetime)

    def test_date_string_is_a_date(self):
        result = convert_date_time("2024-06-01")
        assert result == datetime.date(2024, 6, 1)
        assert not isinstance(result, datetime.datetime)

    def test_datetime_is_unchanged(self):
        dt = datetime.datetime(2024, 6, 1, 0, 0, 0)
        result = convert_date_time(dt)
        assert result == dt and isinstance(result, datetime.datetime)

    def test_date_is_unchanged(self):
        result = convert_date_time(datetime.date(2024, 6, 1))
        assert result == datetime.date(2024, 6, 1) and not isinstance(result, datetime.datetime)

    def test_midnight_datetime_stays_a_datetime(self):
        assert isinstance(convert_date_time("2024-06-01 00:00:00"), datetime.datetime)

    def test_bad_string_raises(self):
        with pytest.raises(ValueError, match="Processing date string"):
            convert_date_time("01/06/2024")

    def test_bad_type_raises(self):
        with pytest.raises(TypeError):
            convert_date_time(20240601)

    def test_find_all_keeps_a_date_processing_date(self):
        result = convert_inputs_and_select(None, "2024-06-01", [], Table("trades", []), NoOperation())
        assert result._processing_datetime == datetime.date(2024, 6, 1)
        assert not isinstance(result._processing_datetime, datetime.datetime)


class TestOrderByAccumulates:
    """Chained order_by calls accumulate: the latest call's sorts are primary and earlier ones break their ties."""

    @staticmethod
    def _result() -> FinderResult:
        return _make_result()

    @staticmethod
    def _columns(result: FinderResult) -> list[tuple[str, SortDirection]]:
        assert all(isinstance(s.column, ColumnWithJoin) for s in result._order_by)
        return [(s.column.column.name, s.direction) for s in result._order_by]  # type: ignore[attr-defined]

    def test_the_later_call_is_primary(self):
        a = StringAttribute("A", "a", "VARCHAR", "t")
        b = StringAttribute("B", "b", "VARCHAR", "t")
        result = self._result().order_by(a.ascending()).order_by(b.descending())
        assert self._columns(result) == [("b", SortDirection.DESC), ("a", SortDirection.ASC)]

    def test_one_call_keeps_the_order_it_was_given(self):
        a = StringAttribute("A", "a", "VARCHAR", "t")
        b = StringAttribute("B", "b", "VARCHAR", "t")
        result = self._result().order_by(a.ascending(), b.descending())
        assert self._columns(result) == [("a", SortDirection.ASC), ("b", SortDirection.DESC)]

    def test_three_calls_run_newest_first(self):
        a, b, c = (StringAttribute(n.upper(), n, "VARCHAR", "t") for n in "abc")
        result = self._result().order_by(a.ascending()).order_by(b.ascending()).order_by(c.ascending())
        assert [name for name, _ in self._columns(result)] == ["c", "b", "a"]

    def test_sorting_a_column_again_replaces_its_earlier_direction(self):
        a = StringAttribute("A", "a", "VARCHAR", "t")
        b = StringAttribute("B", "b", "VARCHAR", "t")
        result = self._result().order_by(a.ascending(), b.ascending()).order_by(a.descending())
        assert self._columns(result) == [("a", SortDirection.DESC), ("b", SortDirection.ASC)]

    def test_the_same_column_reached_through_a_join_is_a_different_sort(self):
        join = JoinOperation("Other", Table("other", []), Column("fk", "INT", "t"), Column("id", "INT", "other"))
        own = StringAttribute("Name", "name", "VARCHAR", "t")
        joined = StringAttribute("Other Name", "name", "VARCHAR", "other", JoinTreeNodeOperation(join))
        result = self._result().order_by(own.ascending()).order_by(joined.ascending())
        assert len(result._order_by) == 2

    def test_no_calls_means_no_ordering(self):
        assert self._result()._order_by == []


class TestDistinct:

    def test_not_distinct_by_default(self):
        assert _make_result()._distinct is False

    def test_distinct_returns_the_result_for_chaining(self):
        result = _make_result()
        assert result.distinct() is result and result._distinct is True

    def test_a_view_cannot_be_distinct(self):
        from model.view import ViewDefinitionError
        with pytest.raises(ViewDefinitionError, match="distinct"):
            _make_result().distinct().to_view("v", "s")


class TestOffset:

    def test_offset_and_slice(self):
        assert _make_result().offset(3)._offset == 3
        sliced = _make_result().slice(2, 7)
        assert (sliced._offset, sliced._limit) == (2, 5)

    def test_bad_values_are_refused(self):
        with pytest.raises(ValueError):
            _make_result().offset(-1)
        with pytest.raises(ValueError):
            _make_result().slice(5, 2)
        with pytest.raises(ValueError):
            _make_result().slice(-1, 2)

    def test_a_view_has_no_offset(self):
        from model.view import ViewDefinitionError
        with pytest.raises(ViewDefinitionError, match="offset"):
            _make_result().offset(1).to_view("v", "s")


class TestDuckDbTimeout:
    """DuckDbConnect.select() only builds the (unexecuted) query plan — the timeout can only
    fire once something actually materializes it, via to_pandas() or to_numpy()."""

    def _output(self, monkeypatch):
        import duckdb
        from datafinder_duckdb.duckdb_engine import DuckDbConnect
        import datafinder_duckdb.duckdb_engine as engine_module

        conn = duckdb.connect()

        monkeypatch.setattr(duckdb, 'connect', lambda *a, **kw: conn)
        monkeypatch.setattr(engine_module, 'to_sql',
                            lambda *a, **kw: "SELECT sum(i) FROM range(1000000000) t(i)")

        return DuckDbConnect.select(
            datetime.date(2024, 1, 1), datetime.datetime(2024, 1, 1, 0, 0, 0),
            [], Table("trades", []), NoOperation(), timeout_ms=1)

    def test_select_itself_does_not_execute_or_raise(self, monkeypatch):
        # No timeout here: select() must not touch the connection, just build the plan.
        self._output(monkeypatch)

    def test_to_numpy_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_numpy()

    def test_to_pandas_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_pandas()


def _view() -> View:
    return View("trades_v", "reporting", Table("trades", []), [Alias(Column("sym", "VARCHAR", "trades"), "symbol")])


class _Rows(DataFrame):
    FRAME = pd.DataFrame({"symbol": ["AAPL"]})
    ARRAY = np.array([["AAPL"]], dtype=object)

    def to_pandas(self) -> pd.DataFrame:
        return self.FRAME

    def to_numpy(self) -> np.ndarray:
        return self.ARRAY


class TestViewResult:

    @pytest.fixture
    def runner(self):
        """A runner that records how it was asked to create and select a view."""
        calls = []
        QueryRunnerBase.clear()

        class RecordingRunner(QueryRunnerBase):
            @staticmethod
            def create_view(view):
                calls.append(("create", view))

            @staticmethod
            def select_view(view, order_by=None, timeout_ms=_DEFAULT_TIMEOUT_MS):
                calls.append(("select", view, order_by, timeout_ms))
                return _Rows()

        yield calls
        QueryRunnerBase.clear()

    def test_default_timeout_matches_finder_result(self):
        assert ViewResult(_view())._timeout_ms == _DEFAULT_TIMEOUT_MS

    def test_timeout_sets_value_and_returns_self(self):
        result = ViewResult(_view())
        assert result.timeout(500) is result
        assert result._timeout_ms == 500

    def test_to_sql_is_the_create_view_ddl(self):
        assert ViewResult(_view()).to_sql() == (
            'CREATE OR REPLACE VIEW reporting.trades_v AS SELECT t0.sym AS "symbol" FROM trades AS t0')

    def test_to_sql_can_be_transpiled(self):
        assert "`symbol`" in ViewResult(_view()).to_sql(dialect="databricks")

    def test_nothing_runs_until_rows_are_asked_for(self, runner):
        ViewResult(_view()).to_sql()
        assert runner == []

    def test_rows_are_read_by_creating_the_view_then_selecting_from_it(self, runner):
        view = _view()
        result = ViewResult(view).order_by("symbol").timeout(250)
        assert result.to_pandas() is _Rows.FRAME
        assert result.to_numpy() is _Rows.ARRAY
        assert runner == [("create", view), ("select", view, ["symbol"], 250)] * 2

    def test_order_by_accepts_only_columns_of_the_view(self):
        with pytest.raises(ValueError, match="Unknown view columns"):
            ViewResult(_view()).order_by("nope")

    def test_a_runner_that_does_not_support_views_says_so(self):
        with pytest.raises(NotImplementedError):
            QueryRunnerBase.create_view(_view())
        with pytest.raises(NotImplementedError):
            QueryRunnerBase.select_view(_view())

    def test_reading_rows_fails_on_a_runner_that_cannot_create_views(self):
        QueryRunnerBase.clear()

        class QueryOnlyRunner(QueryRunnerBase):
            pass

        try:
            with pytest.raises(NotImplementedError):
                ViewResult(_view()).to_pandas()
        finally:
            QueryRunnerBase.clear()
