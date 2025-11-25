import datetime

import duckdb
import numpy

from calc.calc_protocol import CalcEngineRegistry
from numpy.testing import assert_array_almost_equal

from datafinder import QueryRunnerBase
from datafinder_ibis.ibis_engine import IbisConnect
from mappings import generate_mappings
from setup_test_data import setup_duckdb


class TestCalc:

    def setup(self):
        #Register the Ibis engine
        QueryRunnerBase.clear()
        QueryRunnerBase.register(IbisConnect)
        assert QueryRunnerBase.get_runner() == IbisConnect

        generate_mappings()
        setup_duckdb()

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
        input_data = ContractualPositionFinder.find_all(datetime.date(2024,1,10),inputs).to_numpy()

        output = calc.calculate(input_data)

        assert_array_almost_equal(output, numpy.array([240700.0]), decimal=2)




