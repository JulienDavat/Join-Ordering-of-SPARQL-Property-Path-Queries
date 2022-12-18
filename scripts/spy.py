from pandas import DataFrame
from typing import Any


class Spy():

    def __init__(self) -> None:
        self._rows = {}
        self._columns = set()

    def has(self, row: str, column: str) -> bool:
        if row not in self._rows or column not in self._rows[row]:
            return False
        return True

    def report(self, row: str, column: str, value: Any) -> None:
        self._rows.setdefault(row, {})
        self._rows[row][column] = value
        self._columns.add(column)

    def get(self, row: str, column: str) -> Any:
        return self._rows[row][column]

    def get_default(self, row: str, column: str, default: Any) -> Any:
        if not self.has(row, column):
            return default
        return self.get(row, column)

    def to_dataframe(self) -> DataFrame:
        columns = list(sorted(self._columns))
        rows = []
        for row in self._rows.values():
            rows.append([value for _, value in sorted(row.items())])
        return DataFrame(rows, columns=columns)

    def to_csv(self, filename: str) -> None:
        df = self.to_dataframe()
        df.to_csv(filename, index=False)
