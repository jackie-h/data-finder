import duckdb
import datetime

import numpy as np

from datafinder import QueryRunnerBase
from datafinder_ibis.ibis_engine import IbisConnect
from example import queries
from numpy.testing import assert_array_equal

from mappings import generate_mappings


class TestDataFinderIbisDuckDb:

    def setup(self):
        #Register the Ibis engine
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        assert QueryRunnerBase.get_runner() == IbisConnect
        generate_mappings()
        con = duckdb.connect('test.db')
        con.sql("SELECT 42 AS x").show()
        con.execute("DROP TABLE IF EXISTS trades;")
        con.execute(
            "CREATE TABLE trades(id INT, account_id INT, sym VARCHAR, price DOUBLE, start_at TIMESTAMP, end_at TIMESTAMP); COPY trades FROM 'data/trades.csv'")
        con.sql("SELECT * from trades").show()
        con.sql("SELECT * from trades where sym LIKE 'AAPL'").show()

        con.execute("DROP TABLE IF EXISTS account_master;")
        con.execute(
            "CREATE TABLE account_master(id INT, name VARCHAR); COPY account_master FROM 'data/accounts.csv'")

        con.execute("DROP TABLE IF EXISTS contractualposition;")
        con.execute(
            "CREATE TABLE contractualposition(DATE DATE, INSTRUMENT VARCHAR, CPTY_ID INT, QUANTITY DOUBLE); COPY contractualposition FROM 'data/contractualpositions.csv'")
        con.sql("SELECT * from contractualposition").show()


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


    def test_milestoning_queries(self):
        from trade_finder import TradeFinder
        trades_with_account = TradeFinder.find_all(datetime.datetime.strptime('2020-01-01 09:00:00', '%Y-%m-%d %H:%M:%S'),
                                                   [TradeFinder.account().name(), TradeFinder.symbol(),
                                                    TradeFinder.price()],
                                                   TradeFinder.symbol().eq("IBM"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'IBM', 1203.5]], dtype=object))

        trades_with_account = TradeFinder.find_all(
            datetime.datetime.strptime('2022-01-01 10:00:00', '%Y-%m-%d %H:%M:%S'),
            [TradeFinder.account().name(), TradeFinder.symbol(),
             TradeFinder.price()],
            TradeFinder.symbol().eq("IBM"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'IBM', 3000.5]], dtype=object))

    def test_milestoning_single_business_date_operations(self):
        from contractualposition_finder import ContractualPositionFinder
        positions = ContractualPositionFinder.find_all(datetime.date(2024,1,11),
                                                       [ContractualPositionFinder.instrument().symbol(), ContractualPositionFinder.quantity()])
        np_pos = positions.to_numpy()
        print(np_pos)
        assert_array_equal(np_pos, np.array([['GS', 1000.0]], dtype=object))

        positions = ContractualPositionFinder.find_all(datetime.date(2024,1,10),
                                                       [ContractualPositionFinder.instrument().symbol(), ContractualPositionFinder.quantity()])
        np_pos = positions.to_numpy()
        print(np_pos)
        assert_array_equal(np_pos, np.array([['IBM', 200.0]], dtype=object))