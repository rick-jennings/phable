from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from io import StringIO
from typing import Any, Mapping, Sequence

from phable.grid_builder import GridBuilder
from phable.io.ph_codec import PhCodec
from phable.io.ph_tokenizer import PhToken, PhTokenizer, is_literal
from phable.io.ph_tz import _tz_iana_to_haystack
from phable.kinds import (
    NA,
    Coord,
    Grid,
    GridCol,
    Marker,
    Number,
    PhKind,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
)


class ZincCodec(PhCodec):
    """Encode and decode Haystack data in Zinc format."""

    media_type = "text/zinc"

    def to_str(self, data: PhKind) -> str:
        out = StringIO()
        if isinstance(data, Grid):
            _write_grid(out, data)
        else:
            _write_val(out, data)
        return out.getvalue()

    def from_str(self, data: str) -> PhKind:
        return _ZincParser(data).read()


# ── Encoding ──────────────────────────────────────────────────────────────────


def _write_val(out: StringIO, val: Any) -> None:
    if val is None:
        out.write("N")
    elif isinstance(val, Grid):
        _write_nested_grid(out, val)
    elif isinstance(val, list):
        _write_list(out, val)
    elif isinstance(val, dict):
        _write_dict(out, val)
    else:
        _write_scalar(out, val)


def _write_grid(out: StringIO, grid: Grid) -> None:
    _write_meta(out, False, grid.meta)
    out.write("\n")

    if len(grid.cols) == 0:
        out.write("noCols\n")
    else:
        col_names = []
        for i, col in enumerate(grid.cols):
            if i > 0:
                out.write(",")
            _write_col(out, col)
            col_names.append(col.name)
        out.write("\n")

    for row in grid.rows:
        _write_row(out, row, col_names)

    out.write("\n")


def _write_col(out: StringIO, col: GridCol) -> None:
    out.write(col.name)
    if col.meta is not None:
        _write_meta(out, True, col.meta)


def _write_row(
    out: StringIO, row: Mapping[str, Any], col_names: Sequence[str]
) -> None:
    for index, col_name in enumerate(col_names):
        if index > 0:
            out.write(",")
        try:
            val = row.get(col_name)
            if val is None and len(col_names) == 1:
                out.write("N")
            elif val is None and len(col_names) > 1:
                continue
            else:
                _write_val(out, val)
        except Exception:
            raise IOError(f"Cannot write col '{col_name}' = '{val}'")
    out.write("\n")


def _write_meta(out: StringIO, leading_space: bool, m: Mapping) -> None:
    for key, val in m.items():
        if leading_space:
            out.write(" ")
        else:
            leading_space = True
        out.write(key)
        try:
            if val != Marker():
                out.write(":")
                _write_val(out, val)
        except Exception:
            raise IOError(f"Cannot write meta {key}: {val}")


def _write_nested_grid(out: StringIO, grid: Grid) -> None:
    out.write("<<\n")
    _write_grid(out, grid)
    out.write(">>")


def _write_list(out: StringIO, lst: list[Any]) -> None:
    out.write("[")
    for i, val in enumerate(lst):
        if i > 0:
            out.write(",")
        _write_val(out, val)
    out.write("]")


def _write_dict(out: StringIO, d: dict) -> None:
    out.write("{")
    _write_meta(out, False, d)
    out.write("}")


def _write_scalar(out: StringIO, val: Any) -> None:
    match val:
        case str():
            s = _escape_str(val)
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
            s = f"{val.type}({_escape_str(val.val)})"
        case Uri():
            s = _escape_uri(val)
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
            raise ValueError(f"Cannot write scalar {val!r} of type {type(val).__name__}")

    out.write(s)


def _escape_str(x: str) -> str:
    parts = ['"']
    for c in x:
        match c:
            case "\n":
                parts.append(r"\n")
            case "\r":
                parts.append(r"\r")
            case "\f":
                parts.append(r"\f")
            case "\t":
                parts.append(r"\t")
            case "\\":
                parts.append(r"\\")
            case '"':
                parts.append(r"\"")
            case "`":
                parts.append(r"\`")
            case "'":
                parts.append(r"\'")
            case _:
                ord_num = ord(c)
                if ord_num > 127:
                    parts.append("\\u" + hex(ord_num).replace("0x", ""))
                else:
                    parts.append(c)
    parts.append('"')
    return "".join(parts)


def _escape_uri(x: Uri) -> str:
    parts = ["`"]
    for c in x.val:
        match c:
            case "\n":
                parts.append(r"\n")
            case "\r":
                parts.append(r"\r")
            case "\f":
                parts.append(r"\f")
            case "\t":
                parts.append(r"\t")
            case "`":
                parts.append(r"\`")
            case _:
                parts.append(c)
    parts.append("`")
    return "".join(parts)


# ── Decoding ──────────────────────────────────────────────────────────────────


class _ZincParser:
    """Stateful Zinc parser. One instance per loads() call."""

    def __init__(self, s: str) -> None:
        self._tokenizer = PhTokenizer(StringIO(s))
        self._cur: PhToken = PhToken.EOF
        self._cur_val: Any | None = None
        self._cur_line: int | None = None
        self._peek: PhToken = PhToken.EOF
        self._peek_val: Any | None = None
        self._peek_line: int | None = None
        self._consume()
        self._consume()

    def read(self) -> Any:
        try:
            if self._cur == PhToken.ID and self._cur_val == "ver":
                val = self._parse_grid()
            else:
                val = self._parse_val()
            self._verify(PhToken.EOF)
            return val
        finally:
            self._tokenizer.close()

    def _parse_val(self) -> Any:
        if self._cur == PhToken.ID:
            id = str(self._cur_val)
            self._consume()

            if self._cur == PhToken.LPAREN:
                if self._peek == PhToken.NUM:
                    return self._parse_coord(id)
                else:
                    return self._parse_xstr(id)

            match id:
                case "T":
                    return True
                case "F":
                    return False
                case "N":
                    return None
                case "M":
                    return Marker()
                case "NA":
                    return NA()
                case "R":
                    return Remove()
                case "NaN":
                    return Number(float("nan"))
                case "INF":
                    return Number(float("inf"))

            raise ValueError(f"Unexpected identifier {id}")

        if is_literal(self._cur):
            return self._parse_literal()

        if self._cur == PhToken.MINUS and self._peek_val == "INF":
            self._consume()
            self._consume()
            return Number(float("-inf"))

        if self._cur == PhToken.LBRACKET:
            return self._parse_list()
        if self._cur == PhToken.LBRACE:
            return self._parse_dict(True)
        if self._cur == PhToken.LT2:
            return self._parse_grid()

        raise ValueError(f"Unexpected token: {self._cur}")

    def _parse_literal(self) -> Any:
        val = self._cur_val
        if self._cur == PhToken.REF and self._peek == PhToken.STR:
            assert isinstance(val, Ref)
            val = self._tokenizer.factory.make_ref(val.val, self._peek_val)
            self._consume()
        self._consume()
        return val

    def _parse_coord(self, id: str) -> Coord:
        if id != "C":
            raise Exception(f"Expecting 'C' for coord, not {id}")
        self._consume(PhToken.LPAREN)
        lat = self._consume_num()
        self._consume(PhToken.COMMA)
        lng = self._consume_num()
        self._consume(PhToken.RPAREN)
        return Coord(Decimal(str(lat.val)), Decimal(str(lng.val)))

    def _parse_xstr(self, id: str) -> XStr:
        if not id[0].isupper():
            raise Exception(f"Invalid XStr type {id}")
        self._consume(PhToken.LPAREN)
        val = self._consume_str()
        self._consume(PhToken.RPAREN)
        return XStr(id, val)

    def _parse_list(self) -> list[Any]:
        acc = []
        self._consume(PhToken.LBRACKET)
        while self._cur != PhToken.RBRACKET and self._cur != PhToken.EOF:
            val = self._parse_val()
            acc.append(val)
            if self._cur != PhToken.COMMA:
                break
            self._consume()
        self._consume(PhToken.RBRACKET)
        return acc

    def _parse_dict(self, allow_comma: bool) -> dict[str, Any]:
        acc = {}
        braces = self._cur == PhToken.LBRACE
        if braces:
            self._consume(PhToken.LBRACE)

        while self._cur == PhToken.ID:
            id = str(self._cur_val)
            if not id[0].islower() and id[0] != "_":
                raise Exception(f"Invalid dict tag name: {id}")
            self._consume()

            val = Marker()
            if self._cur == PhToken.COLON:
                self._consume()
                val = self._parse_val()

            acc[id] = val

            if allow_comma and self._cur == PhToken.COMMA:
                self._consume()

        if braces:
            self._consume(PhToken.RBRACE)

        for key in list(acc.keys()):
            if acc[key] is None:
                acc.pop(key)

        return acc

    def _parse_grid(self) -> Any:
        nested = self._cur == PhToken.LT2

        if nested:
            self._consume(PhToken.LT2)
            if self._cur == PhToken.NL:
                self._consume(PhToken.NL)

        if self._cur != PhToken.ID or self._cur_val != "ver":
            raise ValueError(f"Expecting grid 'ver' identifier, not {self._cur}")
        self._consume()
        self._consume(PhToken.COLON)
        self._check_version(self._consume_str())

        gb = GridBuilder()
        if self._cur == PhToken.ID:
            gb.set_meta(self._parse_dict(False))
        self._consume(PhToken.NL)

        while self._cur == PhToken.ID:
            name = self._consume_tag_name()
            meta = None
            if self._cur == PhToken.ID:
                meta = self._parse_dict(False)
            gb.add_col(name, meta)
            if self._cur != PhToken.COMMA:
                break
            self._consume(PhToken.COMMA)

        if not gb.col_names:
            raise ValueError("No columns defined")
        self._consume(PhToken.NL)

        while True:
            if self._cur == PhToken.NL:
                break
            if self._cur == PhToken.EOF:
                break
            if nested and self._cur == PhToken.GT2:
                break

            row = {}
            num_cols = len(gb.col_names)

            for i, col_name in enumerate(gb.col_names):
                if (
                    self._cur == PhToken.COMMA
                    or self._cur == PhToken.NL
                    or self._cur == PhToken.EOF
                ):
                    row[col_name] = None
                else:
                    row[col_name] = self._parse_val()
                if i + 1 < num_cols:
                    self._consume(PhToken.COMMA)

            for key in list(row.keys()):
                if row[key] is None:
                    row.pop(key)

            if len(row) > 0:
                gb.add_row(row)

            if nested and self._cur == PhToken.GT2:
                break
            if self._cur == PhToken.EOF:
                break
            self._consume(PhToken.NL)

        if self._cur == PhToken.NL:
            self._consume()
        if nested:
            self._consume(PhToken.GT2)
        return gb.build()

    def _check_version(self, s: str) -> int:
        if s == "3.0":
            return 3
        raise ValueError(f"Unsupported version {s}")

    def _consume_tag_name(self) -> str:
        self._verify(PhToken.ID)
        id = str(self._cur_val)
        if not id[0].islower() and id[0] != "_":
            raise ValueError(f"Invalid dict tag name: {id}")
        self._consume()
        return id

    def _consume_num(self) -> Number:
        val = self._cur_val
        assert isinstance(val, Number)
        self._consume(PhToken.NUM)
        return val

    def _consume_str(self) -> str:
        val = self._cur_val
        assert isinstance(val, str)
        self._consume(PhToken.STR)
        return val

    def _verify(self, expected: PhToken) -> None:
        if self._cur != expected:
            raise ValueError(f"Expected {expected} not {self._cur}")

    def _consume(self, expected: PhToken | None = None) -> None:
        if expected is not None:
            self._verify(expected)

        self._cur = self._peek
        self._cur_val = self._peek_val
        self._cur_line = self._peek_line

        self._peek = self._tokenizer.next()
        self._peek_val = self._tokenizer.val
        self._peek_line = self._tokenizer.line
