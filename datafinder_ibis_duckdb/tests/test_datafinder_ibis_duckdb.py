import duckdb
import datetime

import numpy as np

from datafinder import QueryRunnerBase
from datafinder_ibis.ibis_engine import IbisConnect
from example import queries
from numpy.testing import assert_array_equal

from mappings import generate_mappings
from setup_test_data import setup_duckdb


class TestDataFinderIbisDuckDb:

    def setup(self):
        #Register the Ibis engine
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        assert QueryRunnerBase.get_runner() == IbisConnect
        generate_mappings()
        setup_duckdb()


    def test_queries(self):
        self.setup()
        # Import after generation, so we get the latest version
        from trade_finder import TradeFinder
        tf = TradeFinder()
        queries.find_trades(tf)
        from account_finder import AccountFinder
        af = AccountFinder()
        np_accts = af \
            .find_all(None, None, [af.id_(), af.name()],
                      af.id_().eq(211978)) \
            .to_numpy()
        print(np_accts)
        assert_array_equal(np_accts, np.array([[211978, 'Trading Account 1']],dtype=object))


        trades_with_account = tf.find_all(None, datetime.datetime.now(),
                                          [tf.account().name(),
                                           tf.account().id_(),
                                           tf.symbol(),
                                           tf.price()],
                                           tf.symbol().eq("AAPL"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 211978, 'AAPL', 84.11]], dtype=object))

    def test_pandas(self):
        self.setup()
        # Import after generation, so we get the latest version
        from trade_finder import TradeFinder
        tf = TradeFinder()
        queries.find_trades(tf)
        from account_finder import AccountFinder
        af = AccountFinder()
        df = af \
            .find_all(None, None, [af.id_(), af.name()],
                      af.id_().eq(211978)) \
            .to_pandas()
        print(df)

        assert_array_equal(df.columns, ['Id', 'Name'])
        assert df.values[0][0] == 211978
        assert df.values[0][1] == 'Trading Account 1'

        trades_with_account = tf.find_all(None, datetime.datetime.now(),
                                          [tf.account().name(),
                                           tf.account().id_(),
                                           tf.symbol(),
                                           tf.price()],
                                           tf.symbol().eq("AAPL"))
        df2 = trades_with_account.to_pandas()
        assert_array_equal(df2.columns, ['Account Name', 'Account Id', 'Symbol', 'Price'])
        assert_array_equal(df2.values, np.array([['Trading Account 1', 211978, 'AAPL', 84.11]], dtype=object))


    def test_milestoning_queries(self):
        self.setup()
        from trade_finder import TradeFinder
        tf = TradeFinder()
        trades_with_account = tf.find_all(None, '2020-01-01 09:00:00',
                                          [tf.account().name(),
                                           tf.instrument().symbol(),
                                           tf.price()],
                                           tf.symbol().eq("IBM"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'IBM', 1203.5]], dtype=object))

        trades_with_account = tf.find_all(None, '2022-01-01 10:00:00',
            [tf.account().name(), tf.symbol(), tf.price()],
            tf.symbol().eq("IBM"))
        np_trades = trades_with_account.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([['Trading Account 1', 'IBM', 3000.5]], dtype=object))

    def test_milestoning_single_business_date_operations(self):
        from contractualposition_finder import ContractualPositionFinder
        cpf = ContractualPositionFinder()
        positions = cpf.find_all(datetime.date(2024,1,11),
                                 '2022-01-01 10:00:00',
                                 [cpf.instrument().symbol(),
                                  cpf.instrument().price(),
                                  cpf.quantity()])
        np_pos = positions.to_numpy()
        print(np_pos)
        assert_array_equal(np_pos, np.array([['GS', 45.7, 1000.0]], dtype=object))

        positions = cpf.find_all('2024-01-10',
                                 '2022-01-01 10:00:00',
                                 [cpf.instrument().symbol(),
                                  cpf.quantity()])
        np_pos = positions.to_numpy()
        print(np_pos)
        assert_array_equal(np_pos, np.array([['IBM', 200.0]], dtype=object))

    def test_aggregate_columns(self):
        self.setup()
        # Import after generation, so we get the latest version
        from trade_finder import TradeFinder
        tf = TradeFinder()
        trades_sum = tf.find_all(None, datetime.datetime.now(),
                                 [tf.price().sum()])
        np_trades = trades_sum.to_numpy()
        print(np_trades)
        assert_array_equal(np_trades, np.array([[3130.31]], dtype=object))
    def test_find_for_date_range_single_business_date(self):
        self.setup()
        from contractualposition_finder import ContractualPositionFinder
        cpf = ContractualPositionFinder()
        # processing_valid_at milestones the joined instrument/price table to one row per symbol
        processing_dt = '2022-01-01 10:00:00'

        # Range covering both dates returns both positions
        positions = cpf.find_for_date_range(
            '2024-01-10', '2024-01-11', processing_dt,
            [cpf.instrument().symbol(), cpf.quantity()])
        instruments = sorted(positions.to_pandas()["Instrument Symbol"].tolist())
        assert instruments == ['GS', 'IBM']

        # Range covering only the first date returns only IBM
        positions = cpf.find_for_date_range(
            '2024-01-10', '2024-01-10', processing_dt,
            [cpf.instrument().symbol(), cpf.quantity()])
        np_pos = positions.to_numpy()
        assert_array_equal(np_pos, np.array([['IBM', 200.0]], dtype=object))

        # Range covering only the second date returns only GS
        positions = cpf.find_for_date_range(
            '2024-01-11', '2024-01-11', processing_dt,
            [cpf.instrument().symbol(), cpf.quantity()])
        np_pos = positions.to_numpy()
        assert_array_equal(np_pos, np.array([['GS', 1000.0]], dtype=object))

        # Range before any data returns nothing
        positions = cpf.find_for_date_range(
            '2024-01-01', '2024-01-09', processing_dt,
            [cpf.instrument().symbol(), cpf.quantity()])
        assert len(positions.to_pandas()) == 0
