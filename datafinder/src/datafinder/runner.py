from datafinder import Attribute, Operation, DataFrame


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


class QueryRunnerBase(metaclass=RegistryBase):

    @staticmethod
    def select(columns: list[Attribute], table: str, op: Operation) -> DataFrame:
        pass

    @staticmethod
    def get_runner():
        for k in RegistryBase.REGISTRY.keys():
            if k != 'QueryRunnerBase':
                return RegistryBase.REGISTRY[k]
        raise Exception("No query runner registered")
