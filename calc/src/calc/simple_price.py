from calc.calc_protocol import DomainAwareCalc
from contractualposition_finder import ContractualPositionFinder
from datafinder import Attribute
import numpy as np


def npv(quantity: np.ndarray, price: np.ndarray):
    return quantity * price


_position_finder = ContractualPositionFinder()


class PositionNPVCalc(DomainAwareCalc):

    def name(self) -> str:
        return 'npv'

    def inputs_spec(self) -> list[Attribute]:
        return [_position_finder.quantity(),
                _position_finder.instrument().price()]

    def calculate(self, inputs: np.ndarray) -> np.ndarray:  # type: ignore[override]
        return npv(inputs[0][0], inputs[0][1])

    def output_spec(self) -> Attribute:
        return _position_finder.npv()
