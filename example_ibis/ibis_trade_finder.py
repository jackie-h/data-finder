from datafinder.typed_attributes import *
from datafinder_ibis.ibis_engine import *


class TradeFinder:
    __table = 'trade'
    __symbol = StringAttribute('sym', 'char')
    __price = FloatAttribute('price', 'double precision')

    @staticmethod
    def symbol() -> StringAttribute:
        return TradeFinder.__symbol

    @staticmethod
    def price() -> FloatAttribute:
        return TradeFinder.__price

    @staticmethod
    def find_all(date_from: datetime.date, date_to: datetime.date, as_of: str,
                 display_columns: list[StringAttribute],
                 filter_op: Operation) -> DataFrame:

        out = IbisConnect.select(display_columns, TradeFinder.__table, filter_op)
        return IbisOutput(out)
