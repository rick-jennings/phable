from datetime import date, datetime, time
from io import StringIO
from typing import Any, Self

from phable import NA, Coord, Grid, Marker, Number, Ref, Remove, Symbol, Uri, XStr
from phable.parsers.json import _tz_iana_to_haystack


class ZincWriter:
    """Write Haystack data in Zinc format."""

    def __init__(self, out: StringIO):
        self._out = out

    _out: StringIO

    @staticmethod
    def grid_to_str(grid: Grid) -> str:
        """Format a grid to a zinc string in memory."""
        buf = StringIO()
        ZincWriter(buf).write_grid(grid)
        return buf.getvalue()

    @staticmethod
    def val_to_str(val: Any) -> str:
        """Get a value as a zinc string."""
        buf = StringIO()
        ZincWriter(buf).write_val(val)
        return buf.getvalue()

    def write_val(self, val: Any) -> None:
        """Write a zinc value."""

        if val is None:
            self._out.write("N")
        elif isinstance(val, Grid):
            self._write_nested_grid(val)
        elif isinstance(val, list):
            self._write_list(val)
        elif isinstance(val, dict):
            self._write_dict(val)
        else:
            self._write_scalar(val)

    def write_grid(self, grid: Grid) -> Self:
        """Write a grid to a stream."""

        # set meta-data line
        self._write_meta(False, grid.meta)
        self._out.write("\n")

        # columns lines
        if len(grid.cols) == 0:
            # technicially this should be illegal, but
            # for robustness handle it here
            self._out.write("noCols\n")
        else:
            for i, col in enumerate(grid.cols):
                if i > 0:
                    self._out.write(",")
                self._write_col(col)
            self._out.write("\n")

        # rows
        for row in grid.rows:
            self._write_row(row)

        self._out.write("\n")
        return self

    def _write_col(self, col: dict[str, Any]) -> None:
        self._out.write(col["name"])

        if col.get("meta") is not None:
            self._write_meta(True, col["meta"])

    def _write_row(self, row: dict[str, Any]) -> None:
        for index, (key, val) in enumerate(row.items()):
            if index > 0:
                self._out.write(",")
            try:
                if val is None:
                    # if this is only column, then use explicit N for null
                    if index == 0 and len(row) == 1:
                        self._out.write("N")
                else:
                    self.write_val(val)
            except Exception:
                raise IOError(f"Cannot write col '{key}' = '{val}'")
        self._out.write("\n")

    def _write_meta(self, leading_space: bool, m: dict) -> None:
        for key, val in m.items():
            if leading_space:
                self._out.write(" ")
            else:
                leading_space = True
            self._out.write(key)
            try:
                if val != Marker():
                    self._out.write(":")
                    self.write_val(val)
            except Exception:
                raise IOError(f"Cannot write meta {key}: {val}")

    def _write_nested_grid(self, grid: Grid) -> None:
        self._out.write("<")
        self._out.write("<")
        self._out.write("\n")
        self.write_grid(grid)
        self._out.write(">")
        self._out.write(">")

    def _write_list(self, list: list[Any]) -> None:
        self._out.write("[")
        for i, val in enumerate(list):
            if i > 0:
                self._out.write(",")
            self.write_val(val)
        self._out.write("]")

    def _write_dict(self, dict: dict) -> None:
        self._out.write("{")
        self._write_meta(False, dict)
        self._out.write("}")

    def _write_scalar(self, val: Any) -> None:
        match val:
            case str():
                s = self._parse_grid_str_to_zinc_str(val)
            case True:
                s = "T"
            case False:
                s = "F"
            case datetime():
                haystack_tz = _tz_iana_to_haystack(str(val.tzinfo))
                if haystack_tz == "UTC":
                    s = val.isoformat().replace("+00:00", "Z")
                else:
                    s = val.isoformat() + " " + haystack_tz
            case date():
                s = val.isoformat()
            case time():
                s = val.isoformat()
            case Marker():
                s = "M"
            case Ref():
                s = f"@{val.val}"
                if val.dis is not None:
                    s += f' "{val.dis}"'
            case Remove():
                s = "R"
            case Coord():
                s = f"C({val.lat},{val.lng})"
            case XStr():
                s = f"{val.type}({self._parse_grid_str_to_zinc_str(val.val)})"
            case Uri():
                s = self._parse_grid_uri_to_zinc_uri(val)
            case Number():
                if val.val == float("inf"):
                    s = "INF"
                elif val.val == float("-inf"):
                    s = "-INF"
                elif val.val == float("nan"):
                    s = "NaN"
                else:
                    s = f"{val}"
            case NA():
                s = "NA"
            case Symbol():
                s = f"^{val.val}"
            case _:
                raise Exception()

        self._out.write(s)

    def _parse_grid_str_to_zinc_str(self, x: str) -> str:
        # based on the FanStr class in Fantom
        zinc = '"'
        for c in x:
            match c:
                case "\n":
                    zinc += r"\n"
                case "\r":
                    zinc += r"\r"
                case "\f":
                    zinc += r"\f"
                case "\t":
                    zinc += r"\t"
                case "\\":
                    zinc += r"\\"
                case '"':
                    zinc += r"\""
                case "`":
                    zinc += r"\`"
                case "'":
                    zinc += r"\'"
                case _:
                    ord_num = ord(c)
                    if ord_num > 127:
                        zinc += "\\u" + hex(ord_num).replace("0x", "")
                    else:
                        zinc += c

        return zinc + '"'

    def _parse_grid_uri_to_zinc_uri(self, x: Uri) -> str:
        # based on the Uri class in Fantom
        zinc = "`"
        for c in x.val:
            match c:
                case "\n":
                    zinc += r"\n"
                case "\r":
                    zinc += r"\r"
                case "\f":
                    zinc += r"\f"
                case "\t":
                    zinc += r"\t"
                case "`":
                    zinc += r"\`"
                case _:
                    zinc += c

        return zinc + "`"
