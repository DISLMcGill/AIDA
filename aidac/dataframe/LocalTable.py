from __future__ import annotations

import pandas as pd

from aidac import DataFrame


class LocalTable(DataFrame):
    def __init__(self, data, table_name=None):
        super().__init__(table_name)
        self._data_ = data
        self._stub_ = None

    @classmethod
    def read_csv(cls, path, delimiter, header) -> LocalTable:
        df = pd.read_csv(path, delimiter=delimiter, header=header)
        return LocalTable(df)

    def join(self, other: DataFrame, left_on: list | str, right_on: list | str, join_type: str):
        pass