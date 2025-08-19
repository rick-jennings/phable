from __future__ import annotations

from io import TextIOWrapper
from typing import Any

from phable.kinds import NA, Coord, Marker, Number, Remove, XStr
from phable.parsers.grid_builder import GridBuilder
from phable.parsers.haystack_tokenizer import (
    HaystackToken,
    HaystackTokenizer,
    is_literal,
)


class ZincReader:
    def __init__(self, input: TextIOWrapper):
        self._tokenizer = HaystackTokenizer(input)
        self._cur = HaystackToken.EOF
        self._peek = HaystackToken.EOF
        self._consume()
        self._consume()

    _tokenizer: HaystackTokenizer

    _cur: HaystackToken  # current token
    _cur_val: Any | None = None  # current token value
    _cur_line: int  # current token line number

    _peek: HaystackToken  # next token
    _peek_val: Any | None = None  # next token value
    _peek_line: int | None = None  # next token line number

    def close(self) -> None:
        """Close the underlying stream."""
        return self._tokenizer.close()

    def read_val(self, close: bool = True) -> Any:
        """Read a value and auto close the stream."""
        try:
            val: Any
            if self._cur == HaystackToken.ID and self._cur_val == "ver":
                val = self._parse_grid()
            else:
                val = self._parse_val()
            self._verify(HaystackToken.EOF)
            return val
        finally:
            if close:
                self.close()

    def read_tags(self) -> dict[str, Any]:
        """Read a set of tags as 'name:val' pairs separated by space or comma."""
        return self._parse_dict(True)

    def _parse_val(self) -> Any:
        if self._cur == HaystackToken.ID:
            id = str(self._cur_val)
            self._consume()

            # check for coord or xstr
            if self._cur == HaystackToken.LPAREN:
                if self._peek == HaystackToken.NUM:
                    return self._parse_coord(id)
                else:
                    return self._parse_xstr(id)

            # check for keyword
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

        # literals
        if is_literal(self._cur):
            return self._parse_literal()

        # -INF
        if self._cur == HaystackToken.MINUS and self._peek_val == "INF":
            self._consume()
            self._consume()
            return Number(float("-inf"))

        # nested collections
        if self._cur == HaystackToken.LBRACKET:
            return self._parse_list()
        if self._cur == HaystackToken.LBRACE:
            return self._parse_dict(True)
        if self._cur == HaystackToken.LT2:
            return self._parse_grid()

        # give up
        raise ValueError(f"Unexpected token: {self._cur}")

    def _parse_literal(self) -> Any:
        val = self._cur_val
        if self._cur == HaystackToken.REF and self._peek == HaystackToken.STR:
            val = self._tokenizer.factory.make_ref(val.val, self._peek_val)
            self._consume()
        self._consume()
        return val

    def _parse_coord(self, id: str) -> Coord:
        if id != "C":
            raise Exception(f"Expecting 'C' for coord, not {id}")
        self._consume(HaystackToken.LPAREN)
        lat = self._consume_num()
        self._consume(HaystackToken.COMMA)
        lng = self._consume_num()
        self._consume(HaystackToken.RPAREN)
        return Coord(lat.val, lng.val)

    def _parse_xstr(self, id: str) -> XStr:
        if not id[0].isupper():
            raise Exception(f"Invalid XStr type {id}")
        self._consume(HaystackToken.LPAREN)
        val = self._consume_str()
        self._consume(HaystackToken.RPAREN)
        return XStr(id, val)

    def _parse_list(self) -> list[Any]:
        acc = []
        self._consume(HaystackToken.LBRACKET)
        while self._cur != HaystackToken.RBRACKET and self._cur != HaystackToken.EOF:
            val = self._parse_val()
            acc.append(val)
            if self._cur != HaystackToken.COMMA:
                break
            self._consume()
        self._consume(HaystackToken.RBRACKET)
        return acc

    def _parse_dict(self, allow_comma: bool) -> dict[str, Any]:
        # TODO: confirm it's okay for acc to not maintain order
        acc = {}

        braces = self._cur == HaystackToken.LBRACE
        if braces:
            self._consume(HaystackToken.LBRACE)

        while self._cur == HaystackToken.ID:
            # tag name
            id = str(self._cur_val)
            if not id[0].islower() and id[0] != "_":
                raise Exception(f"Invalid dict tag name: {id}")

            self._consume()

            # tag value
            val = Marker()
            if self._cur == HaystackToken.COLON:
                self._consume()
                val = self._parse_val()

            acc[id] = val

            if allow_comma and self._cur == HaystackToken.COMMA:
                self._consume()

        if braces:
            self._consume(HaystackToken.RBRACE)

        for key in list(acc.keys()):
            if acc[key] is None:
                acc.pop(key)

        return acc

    def _parse_grid(self) -> Any:
        nested = self._cur == HaystackToken.LT2

        if nested:
            self._consume(HaystackToken.LT2)
            if self._cur == HaystackToken.NL:
                self._consume(HaystackToken.NL)

        # ver:"3.0"
        if self._cur != HaystackToken.ID or self._cur_val != "ver":
            raise ValueError(f"Expecting grid 'ver' identifier, not {self._cur}")
        self._consume()
        self._consume(HaystackToken.COLON)
        self.ver = self._check_version(self._consume_str())

        # grid meta
        gb = GridBuilder()
        if self._cur == HaystackToken.ID:
            gb.set_meta(self._parse_dict(False))
        self._consume(HaystackToken.NL)

        # column definitions
        while self._cur == HaystackToken.ID:
            name = self._consume_tag_name()
            meta = None
            if self._cur == HaystackToken.ID:
                meta = self._parse_dict(False)
            gb.add_col(name, meta)
            if self._cur != HaystackToken.COMMA:
                break
            self._consume(HaystackToken.COMMA)

        num_cols = gb.num_cols()

        if num_cols == 0:
            raise ValueError("No columns defined")
        self._consume(HaystackToken.NL)

        # grid rows
        while True:
            if self._cur == HaystackToken.NL:
                break
            if self._cur == HaystackToken.EOF:
                break
            if nested and self._cur == HaystackToken.GT2:
                break

            # read cells
            row = {}

            for i, col_name in enumerate(gb.col_names()):
                if (
                    self._cur == HaystackToken.COMMA
                    or self._cur == HaystackToken.NL
                    or self._cur == HaystackToken.EOF
                ):
                    row[col_name] = None
                else:
                    row[col_name] = self._parse_val()
                if i + 1 < num_cols:
                    self._consume(HaystackToken.COMMA)

            for key in list(row.keys()):
                if row[key] is None:
                    row.pop(key)

            if len(row) > 0:
                gb.add_row(row)

            # newline or end
            if nested and self._cur == HaystackToken.GT2:
                break
            if self._cur == HaystackToken.EOF:
                break
            self._consume(HaystackToken.NL)

        if self._cur == HaystackToken.NL:
            self._consume()
        if nested:
            self._consume(HaystackToken.GT2)
        return gb.to_grid()

    def _check_version(self, s: str) -> int:
        if s == "3.0":
            return 3
        raise ValueError(f"Unsupported version {s}")

    def _consume_tag_name(self) -> str:
        self._verify(HaystackToken.ID)
        id = str(self._cur_val)
        if not id[0].islower() and id[0] != "_":
            raise ValueError(f"Invalid dict tag name: {id}")
        self._consume()
        return id

    def _consume_num(self) -> Number:
        val = self._cur_val
        self._consume(HaystackToken.NUM)
        return val

    def _consume_str(self) -> str:
        val = self._cur_val
        self._consume(HaystackToken.STR)
        return val

    def _verify(self, expected: HaystackToken) -> None:
        if self._cur != expected:
            raise ValueError(f"Expected {expected} not {self._cur}")

    def _consume(self, expected: HaystackToken | None = None) -> None:
        if expected is not None:
            self._verify(expected)

        self._cur = self._peek
        self._cur_val = self._peek_val
        self._cur_line = self._peek_line

        self._peek = self._tokenizer.next()
        self._peek_val = self._tokenizer.val
        self._peek_line = self._tokenizer.line
