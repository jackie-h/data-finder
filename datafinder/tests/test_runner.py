import datetime

import pytest

from datafinder.runner import FinderResult, _DEFAULT_TIMEOUT_MS, convert_inputs_and_select_for_date_range
from model.relational import NoOperation, Table


def _make_result() -> FinderResult:
    return FinderResult(None, None, [], None, NoOperation())  # type: ignore[arg-type]


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

        return DuckDbConnect.select(None, None, [], None, None, timeout_ms=1)  # type: ignore[arg-type]

    def test_select_itself_does_not_execute_or_raise(self, monkeypatch):
        # No timeout here: select() must not touch the connection, just build the plan.
        self._output(monkeypatch)

    def test_to_numpy_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_numpy()

    def test_to_pandas_raises_timeout_error(self, monkeypatch):
        with pytest.raises(TimeoutError, match="1ms"):
            self._output(monkeypatch).to_pandas()
