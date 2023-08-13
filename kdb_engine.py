from operation import Operation, QueryEngine
import pykx as kx
import numpy as np
import pandas as pd
class QConnect:

    @staticmethod
    def select(table: str, op: Operation, columns: list[str]) -> kx.Table:
        conn = kx.QConnection('localhost', 5001)
        qe = QueryEngine()
        op.generate_query(qe)
        query = qe.build_query_string()
        print(query)
        res = conn.qsql.select(table, columns, query)
        return res


class Output:
    __table: kx.Table

    def __init__(self, t: kx.Table):
        self.__table = t

    # https://code.kx.com/pykx/1.6/getting-started/quickstart.html#converting-pykx-objects-to-common-python-types
    def to_numpy(self) -> np.array:
        return self.__table.np()

    # https://code.kx.com/pykx/1.6/getting-started/quickstart.html#converting-pykx-objects-to-common-python-types
    def to_pandas(self) -> pd.DataFrame:
        return self.__table.pd()