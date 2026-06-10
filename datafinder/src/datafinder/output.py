import numpy as np
import pandas as pd


class ToNumpy:

    def to_numpy(self) -> np.ndarray:
        raise NotImplementedError()


class DataFrame(ToNumpy):

    def to_pandas(self) -> pd.DataFrame:
        raise NotImplementedError()

