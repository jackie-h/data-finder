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
        from trade_finder import TradeFinder  # type: ignore[import]
        tf = TradeFinder()
        queries.find_trades(tf)
        from account_finder import AccountFinder  # type: ignore[import]
        af = AccountFinder()
        np_accts = af \
            .find_all(None, None, [af.id_(), af.name()],  # type: ignore[arg-type]
                      af.id_().eq(211978)) \
            .to_numpy()
        print(np_accts)
        assert_array_equal(np_accts, np.array([[211978, 'Trading Account 1']],dtype=object))


        trades_with_account = tf.find_all(None, datetime.datetime.now(),  # type: ignore[arg-type]
                                          [tf.account().name(), tf.symbol(),
                                           tf.price()],
                                          tf.symbol().eq("AAPL"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'AAPL', 84.11]], dtype=object))
