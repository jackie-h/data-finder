from calc.calc_protocol import DomainAwareCalc
from contractualposition_finder import ContractualPositionFinder
from datafinder import Attribute
from numpy import array


def npv(quantity: array, price: array):
    return quantity * price


class PositionNPVCalc(DomainAwareCalc):

    def name(self) -> str:
        return 'npv'

    def inputs_spec(self) -> list[Attribute]:
        return [ContractualPositionFinder.quantity(),
                ContractualPositionFinder.instrument().price()]

    def calculate(self, inputs):
        return npv(inputs[0], inputs[1])

    def output_spec(self) -> Attribute:
        return ContractualPositionFinder.npv()
