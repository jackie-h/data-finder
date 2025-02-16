from abc import ABC

from numpy import ndarray

from datafinder import Attribute


class DomainAwareCalcProtocol:
    def name(self):
        raise NotImplementedError

    # TODO should this be an ordered set ? to avoid duplicates
    def inputs_spec(self) -> list[Attribute]:
        raise NotImplementedError

    def output_spec(self) -> Attribute:
        raise NotImplementedError

    #TODO should have key and time?
    def calculate(self, inputs:ndarray) -> []:
        raise NotImplementedError


# From https://charlesreid1.github.io/python-patterns-the-registry.html
class RegistryBase(type):

    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        # instantiate a new type corresponding to the type of class being defined
        # this is currently RegisterBase but in child classes will be the child class
        new_cls = type.__new__(cls, name, bases, attrs)
        cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


class BaseRegisteredClass(metaclass=RegistryBase):
    pass


class CalcEngineRegistry(metaclass=RegistryBase):
    calcs = {}

    @staticmethod
    def register(calc:DomainAwareCalcProtocol):
        CalcEngineRegistry.calcs[calc.name()] = calc


class DomainAwareCalc(DomainAwareCalcProtocol, ABC):

    def __init__(self):
        CalcEngineRegistry.register(self)