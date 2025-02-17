import duckdb
import datetime

import numpy as np

from datafinder import QueryRunnerBase
from example import queries
from numpy.testing import assert_array_equal

from mappings import generate_mappings


class TestDataFinderIbisDuckDb:

    def setup(self):
        #Register the Ibis engine
        from datafinder_ibis.ibis_engine import IbisConnect
        assert QueryRunnerBase.get_runner() == IbisConnect
        generate_mappings()
        con = duckdb.connect('test.db')
        con.sql("SELECT 42 AS x").show()
        con.execute("DROP TABLE IF EXISTS trade;")
        con.execute(
            "CREATE TABLE trade(id INT, account_id INT, sym VARCHAR, price DOUBLE); COPY trade FROM 'data/trades.csv'")
        con.sql("SELECT * from trade").show()
        con.sql("SELECT * from trade where sym LIKE 'AAPL'").show()

        con.execute("DROP TABLE IF EXISTS account;")
        con.execute(
            "CREATE TABLE account(id INT, name VARCHAR); COPY account FROM 'data/accounts.csv'")

    def test_queries(self):
        self.setup()
        # Import after generation, so we get the latest version
        from trade_finder import TradeFinder
        queries.find_trades(TradeFinder)
        from account_finder import AccountFinder
        np_accts = AccountFinder \
            .find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                      [AccountFinder.id(), AccountFinder.name()],
                      AccountFinder.id().eq(211978)) \
            .to_numpy()
        print(np_accts)
        assert_array_equal(np_accts, np.array([[211978, 'Trading Account 1']],dtype=object))


        trades_with_account = TradeFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                                   [TradeFinder.account().name(), TradeFinder.symbol(),
                                                    TradeFinder.price()],
                                                   TradeFinder.symbol().eq("AAPL"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'AAPL', 84.11]], dtype=object))
