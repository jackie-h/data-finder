from datafinder import QueryRunnerBase

class TestIbisEngine:

    def test_initialization(self):
        from datafinder_ibis.ibis_engine import IbisConnect
        out = QueryRunnerBase.get_runner()
        assert out == IbisConnect
