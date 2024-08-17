from datafinder.typed_attributes import *
import numpy as np
import pandas as pd
from datafinder_kdb.kdb_engine import *

class TradeFinder:

    __table = 'trade'
    __symbol = StringAttribute('sym', 'kx_symbol')
    __price = FloatAttribute('price', 'kx_float')

    @staticmethod
    def symbol() -> StringAttribute:
        return TradeFinder.__symbol

    @staticmethod
    def price() -> FloatAttribute:
        return TradeFinder.__price

    @staticmethod
    def find_all(date_from: datetime.date, date_to: datetime.date, as_of: str,
                 filter_op: Operation,
                 display_columns: list[StringAttribute]) -> DataFrame:
        cols = []
        for dc in display_columns:
            cols.append(dc._column_name())
        kx_out = QConnect.select(TradeFinder.__table, filter_op, cols)
        return KxOutput(kx_out)