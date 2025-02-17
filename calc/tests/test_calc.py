import datetime

import duckdb
import numpy

from calc.calc_protocol import CalcEngineRegistry
from numpy.testing import assert_array_almost_equal

from datafinder import QueryRunnerBase
from mappings import generate_mappings


class TestCalc:

    def setup(self):
        #Register the Ibis engine
        from datafinder_ibis.ibis_engine import IbisConnect
        assert QueryRunnerBase.get_runner() == IbisConnect

        generate_mappings()
        con = duckdb.connect('test.db')
        con.execute("DROP TABLE IF EXISTS contractualposition;")
        con.execute(
            "CREATE TABLE contractualposition(DATE DATE, INSTRUMENT VARCHAR, CPTY_ID INT, QUANTITY DOUBLE); COPY contractualposition FROM 'data/contractualpositions.csv'")
        con.sql("SELECT * from contractualposition").show()

        con.execute("DROP TABLE IF EXISTS price;")
        con.execute(
            "CREATE TABLE price(DATE_TIME DATETIME, SYM VARCHAR, PRICE DOUBLE); COPY price FROM 'data/prices.csv'")

    def test_price(self):
        self.setup()
        # TODO - we have to import this first for it to register due to Python's dynamic nature
        # need to do imports to force the load = https://stackoverflow.com/questions/73829483/register-classes-in-different-files-to-a-class-factory
        from calc.simple_price import PositionNPVCalc
        calc = PositionNPVCalc()
        assert len(CalcEngineRegistry.calcs) == 1

        inputs = calc.inputs_spec()

        #Calc run
        from contractualposition_finder import ContractualPositionFinder
        input_data = ContractualPositionFinder.find_all(datetime.date.today(), datetime.date.today(), "LATEST",
                                           inputs).to_numpy()

        output = calc.calculate(input_data)

        assert_array_almost_equal(output, numpy.array([200000.0, 54999.95]), decimal=2)




