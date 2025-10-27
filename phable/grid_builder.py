from __future__ import annotations

from typing import Any, Self

from phable.kinds import Grid


class GridBuilder:
    def __init__(self):
        self._meta = {"ver": "3.0"}
        self._cols = []
        self._rows = []

    _meta: dict[str, Any]
    _cols: list[dict[str, Any]]
    _rows: list[dict[str, Any]]

    def set_meta(self, meta: dict[str, Any]) -> Self:
        self._meta = self._meta | meta.copy()
        return self

    def add_col(self, name: str, meta: dict[str, Any] | None = None):
        if not self._is_tagname(name):
            raise Exception(f"Invalid column name: {name}")

        # verify the column does not already exist
        for c in self._cols:
            if c["name"] == name:
                raise Exception(f"Duplicate column name: {name}")

        col = {"name": name}

        if meta is not None:
            col["meta"] = meta.copy()

        self._cols.append(col)
        return self

    def add_col_names(self, names: list[str]):
        for name in names:
            self.add_col(name)
        return self

    def set_col_meta(self, col_name: str, meta: dict[str, Any]) -> Self:
        col_found = False
        for i, c in enumerate(self._cols):
            if c["name"] == col_name:
                col_found = True
                self._cols[i]["meta"] = self._cols[i]["meta"] | meta.copy()
                break

        if col_found is False:
            raise Exception(f"Column not found: {col_name}")

        return self

    def add_row(self, cells: dict[str, Any]) -> Self:
        if len(cells) > len(self._cols):
            raise Exception(f"Num cells {len(cells)} > Num cols {len(self._cols)}")
        self._rows.append(cells.copy())
        return self

    def to_grid(self) -> Grid:
        return Grid(self._meta, self._cols, self._rows)

    def _is_tagname(self, n: str):
        if len(n) == 0 or n[0].isupper():
            return False
        for c in n:
            if not c.isalnum() and c != "_":
                return False
        return True

    def num_cols(self) -> int:
        return len(self._cols)

    def col_names(self) -> list[str]:
        return [col["name"] for col in self._cols]
