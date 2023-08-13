# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# Interface
class Operation:

    def load_data_source(self, path: str, file_name: str) -> str:
        """Overrides FormalParserInterface.load_data_source()"""
        pass


class EqOperation(Operation):
    __value = ''

    def __init__(self, value):
        self.__value = value


class StringAttribute:

    @staticmethod
    def eq(value: str) -> Operation:
        return EqOperation(value)


class TradeFinder:

    __sym = StringAttribute()

    @staticmethod
    def sym() -> StringAttribute():
        return TradeFinder.__sym

    @staticmethod
    def find_all(op:Operation):
        return []



def find_trades():
    print(f'Finding trades')

    op:Operation = TradeFinder.sym().eq("AAPL")
    trades = TradeFinder.find_all(op)
    print(trades)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    find_trades()


