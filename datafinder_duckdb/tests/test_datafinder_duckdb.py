import duckdb
import datetime

import numpy as np

from datafinder import QueryRunnerBase
from datafinder_duckdb.duckdb_engine import DuckDbConnect
from example import queries
from numpy.testing import assert_array_equal

from mappings import generate_mappings
from setup_test_data import setup_duckdb


class TestDataFinderDuckDb:

    def setup(self):
        #Register the duckdb engine
        QueryRunnerBase.clear()
        QueryRunnerBase.register(DuckDbConnect)
        assert QueryRunnerBase.get_runner() == DuckDbConnect
        generate_mappings()
        setup_duckdb()

    def test_queries(self):
        self.setup()
        # Import after generation, so we get the latest version
        from trade_finder import TradeFinder
        queries.find_trades(TradeFinder)
        from account_finder import AccountFinder
        np_accts = AccountFinder \
            .find_all([AccountFinder.id(), AccountFinder.name()],
                      AccountFinder.id().eq(211978)) \
            .to_numpy()
        print(np_accts)
        assert_array_equal(np_accts, np.array([[211978, 'Trading Account 1']],dtype=object))


        trades_with_account = TradeFinder.find_all(datetime.datetime.now(),
                                                   [TradeFinder.account().name(), TradeFinder.symbol(),
                                                    TradeFinder.price()],
                                                   TradeFinder.symbol().eq("AAPL"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'AAPL', 84.11]], dtype=object))
