from datafinder import QueryRunnerBase
from datafinder_ibis.ibis_engine import IbisConnect


class TestIbisEngine:

    def test_initialization(self):
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        out = QueryRunnerBase.get_runner()
        assert out == IbisConnect
