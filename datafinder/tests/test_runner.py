import pytest

from datafinder.runner import FinderResult, _DEFAULT_TIMEOUT_MS
from model.relational import NoOperation


def _make_result() -> FinderResult:
    return FinderResult(None, None, [], None, NoOperation())


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


class TestDuckDbTimeout:

    def test_query_raises_timeout_error(self, monkeypatch):
        import duckdb
        from datafinder_duckdb.duckdb_engine import DuckDbConnect
        import datafinder_duckdb.duckdb_engine as engine_module

        conn = duckdb.connect()

        monkeypatch.setattr(duckdb, 'connect', lambda *a, **kw: conn)
        monkeypatch.setattr(engine_module, 'to_sql',
                            lambda *a, **kw: "SELECT sum(i) FROM range(1000000000) t(i)")

        with pytest.raises(TimeoutError, match="1ms"):
            DuckDbConnect.select(None, None, [], None, None, timeout_ms=1)
