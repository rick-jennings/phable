from __future__ import annotations

from datetime import date, datetime, time
from enum import StrEnum
from io import TextIOWrapper
from typing import Any

from phable.kinds import Number, Ref, Symbol, Uri
from phable.parsers.json import _haystack_to_iana_tz


class HaystackTokenizer:
    def __init__(self, input: TextIOWrapper):
        self._input = input
        self.tok = HaystackToken.EOF
        self._factory = HaystackFactory()
        self._consume()
        self._consume()

    _input: TextIOWrapper
    _cur: str
    _peek: str = None
    _factory: HaystackFactory

    # Current token type
    tok: HaystackToken

    # Current token value based on type:
    #  - id: identifier string
    #  - literals: the literal value
    #  - keyword: value mapped by self.keywords
    #  - comment: comment line if self.keep_comments set
    val: Any = None

    # One based line number for current token
    line: int = 1

    # Tokenize and return slash-slash comments
    keep_comments: bool = False

    # Tokenize the map's keys as keyword tokens instead of identifiers
    keywords: dict[str, Any] | None = None

    def next(self) -> HaystackToken:
        """Read the next token, store result in self.tok and self.val"""

        # reset
        self.val = None

        # skip non-meaningful whitespace and comments
        while True:
            # treat space, tab, non-breaking space as whitespace
            if self._cur == " " or self._cur == "\t" or self._cur == "\xa0":
                self._consume()
                continue

            # comments
            if self._cur == "/":
                if self._peek == "/" and self.keep_comments:
                    self.tok = self.parse_comment()
                    return self.tok
                if self._peek == "/":
                    self._skip_comment_SL()
                    continue
                if self._peek == "*":
                    self._skip_comment_ML()
                    continue

            break

        # newlines
        if self._cur == "\n" or self._cur == "\r":
            if self._cur == "\r" and self._peek == "\n":
                self._consume()
            self._consume()
            self.line += 1
            self.tok = HaystackToken.NL
            return self.tok

        # handle various starting chars
        if self._cur.isalpha() or (
            self._cur == "_" and (self._peek.isalnum() or self._peek == "_")
        ):
            self.tok = self._id()
            return self.tok
        elif self._cur == '"':
            self.tok = self._str()
            return self.tok
        elif self._cur == "@":
            self.tok = self._ref()
            return self.tok
        elif self._cur == "^":
            self.tok = self._symbol()
            return self.tok
        elif self._cur.isdigit() or (self._cur == "-" and self._peek.isdigit()):
            self.tok = self._num()
            return self.tok
        elif self._cur == "`":
            self.tok = self._uri()
            return self.tok

        # operator
        self.tok = self._operator()
        return self.tok

    # Parse single line comment when keeping comments
    def parse_comment(self) -> HaystackToken:
        s = ""
        self._consume()  # first slash
        self._consume()  # next slash
        if self._cur == " ":
            self._consume()  # first space
        while True:
            if self._cur == "\n" or self._cur == "":
                break
            s += self._cur
            self._consume()
        self.val = s
        return HaystackToken.COMMENT

    # Skip a single line // comment
    def _skip_comment_SL(self) -> None:
        self._consume()  # first slash
        self._consume()  # next slash
        while True:
            if self._cur == "\n" or self._cur == "":
                break
            self._consume()

    # Skip a multi line /* comment.  Note unlike C/Java,
    # slash/star comments can be nested.
    def _skip_comment_ML(self) -> None:
        self._consume()  # first slash
        self._consume()  # next slash
        depth = 1
        while True:
            if self._cur == "*" and self._peek == "/":
                self._consume()
                self._consume()
                depth -= 1
                if depth <= 0:
                    break
            if self._cur == "/" and self._peek == "*":
                self._consume()
                self._consume()
                depth += 1
                continue
            if self._cur == "\n":
                self.line += 1
            if self._cur == "":
                break
            self._consume()

    def _consume(self) -> None:
        self._cur = self._peek
        self._peek = self._input.read(1)

    def _id(self) -> HaystackToken:
        s = ""
        while self._cur.isalnum() or self._cur == "_":
            s += self._cur
            self._consume()

        id = self._factory.make_id(s)

        # check for keyword
        if self.keywords is not None and self.keywords.get(id) is not None:
            self.val = self.keywords[id]
            return HaystackToken.KEYWORD

        # normal id
        self.val = id
        return HaystackToken.ID

    def _str(self) -> HaystackToken:
        self._consume()  # opening quote
        is_triple = self._cur == '"' and self._peek == '"'
        if is_triple:
            self._consume()
            self._consume()

        s = ""
        while True:
            ch = self._cur
            if ch == '"':
                self._consume()
                if is_triple:
                    if self._cur != '"' or self._peek != '"':
                        s += '"'
                        continue
                    self._consume()
                    self._consume()
                break
            if ch == "":
                raise ValueError("Unexpected end of str")
            if ch == "\\":
                s += self._escape()
                continue
            self._consume()
            s += ch

        self.val = self._factory.make_str(s)
        return HaystackToken.STR

    def _num(self) -> HaystackToken:
        # hex number (no unit allowed)
        is_hex = self._cur == "0" and self._peek == "x"
        if is_hex:
            self._consume()
            self._consume()
            s = ""
            while True:
                try:
                    _ = int(self._cur, 16)
                    s += self._cur
                    self._consume()
                    continue
                except ValueError:
                    ...

                if self._cur == "_":
                    self._consume()
                    continue
                break
            self.val = Number(float.fromhex(s))
            return HaystackToken.NUM

        # consume all the things that might be part of this number token
        s = self._cur
        self._consume()
        colons, dashes, unit_index, exp = 0, 0, 0, False

        while True:
            if not self._cur.isdigit():
                if exp and (self._cur == "+" or self._cur == "-"):
                    ...
                elif self._cur == "-":
                    dashes += 1
                elif self._cur == ":" and self._peek.isdigit():
                    colons += 1
                elif (exp or colons >= 1) and self._cur == "+":
                    ...
                elif self._cur == ".":
                    if not self._peek.isdigit():
                        break
                elif (self._cur == "e" or self._cur == "E") and (
                    self._peek == "-" or self._peek == "+" or self._peek.isdigit()
                ):
                    exp = True
                elif (
                    self._cur.isalpha()
                    or self._cur == "%"
                    or self._cur == "$"
                    or self._cur == "/"
                    or (len(self._cur) > 0 and ord(self._cur) > 128)
                ):
                    if unit_index == 0:
                        unit_index = len(s)  # _cur is added to s later
                elif self._cur == "_":
                    if unit_index == 0 and self._peek.isdigit():
                        self._consume()
                        continue
                    else:
                        if unit_index == 0:
                            unit_index = len(s)  # _cur is added to s later
                else:
                    break

            s += self._cur
            self._consume()

        # Date
        if dashes == 2 and colons == 0:
            try:
                self.val = self._factory.make_date(s)
            except Exception:
                raise ValueError(f"Invalid Date literal '{s}'")
            return HaystackToken.DATE

        # Time: we don't require hour to be two digits and
        # we don't require seconds
        if dashes == 0 and colons >= 1:
            if s[1] == ":":
                s = "0" + s
            if colons == 1:
                s = s + ":00"
            try:
                self.val = self._factory.make_time(s)
            except Exception:
                raise ValueError(f"Invalid Time literal '{s}'")

            return HaystackToken.TIME

        # DateTime
        if dashes >= 2:
            # xxx timezone
            if self._cur != " " or not self._peek.isupper():
                if s[-1] == "Z":
                    s += " UTC"
                else:
                    raise ValueError("Expecting timezone")
            else:
                self._consume()
                s += " "
                while (
                    self._cur.isalnum()
                    or self._cur == "_"
                    or self._cur == "-"
                    or self._cur == "+"
                ):
                    s = s + self._cur
                    self._consume()

            try:
                self.val = self._factory.make_datetime(s)
            except Exception:
                raise f"Invalid DateTime literal '{s}'"

            return HaystackToken.DATETIME

        # parse as number
        if unit_index == 0:
            try:
                x = float(s)
            except Exception:
                raise ValueError(f"Invalid Number literal '{s}'")
            self.val = self._factory.make_number(x, None)
        else:
            float_str = s[0:unit_index]
            unit_str = s[unit_index::]

            try:
                x = float(float_str)
            except Exception:
                raise ValueError(f"Invalid Number literal '{s}'")

            self.val = self._factory.make_number(x, unit_str)

        return HaystackToken.NUM

    def _ref(self) -> HaystackToken:
        self._consume()  # @
        s = ""
        while True:
            ch = self._cur
            if _is_ref_id_char(ch):
                self._consume()
                s += ch
            else:
                break

        if len(s) == 0:
            raise ValueError("Invalid empty Ref")

        self.val = self._factory.make_ref(s)
        return HaystackToken.REF

    def _symbol(self) -> HaystackToken:
        self._consume()  # ^
        s = ""
        while True:
            ch = self._cur
            if _is_ref_id_char(ch):
                self._consume()
                s += ch
            else:
                break

        if len(s) == 0:
            raise ValueError("Invalid empty Symbol")

        self.val = self._factory.make_symbol(s)
        return HaystackToken.SYMBOL

    def _uri(self) -> HaystackToken:
        self._consume()  # opening backtick
        s = ""
        while True:
            ch = self._cur
            if ch == "`":
                self._consume()
                break
            if ch == "" or ch == "\n":
                raise ValueError("Unexpected end of uri")
            if ch == "\\":
                match self._peek:
                    case (
                        ":" | "/" | "?" | "#" | "[" | "]" | "@" | "\\" | "&" | "=" | ";"
                    ):
                        s += ch
                        s += self._peek
                        self._consume()
                        self._consume()
                    case _:
                        s += self._escape()
            else:
                self._consume()
                s += ch
        self.val = self._factory.make_uri(s)
        return HaystackToken.URI

    def _escape(self) -> str:
        # consume slash
        self._consume()

        # check basics
        match self._cur:
            case "b":
                self._consume()
                return "\b"
            case "f":
                self._consume()
                return "\f"
            case "n":
                self._consume()
                return "\n"
            case "r":
                self._consume()
                return "\r"
            case "t":
                self._consume()
                return "\t"
            case '"':
                self._consume()
                return '"'
            case "$":
                self._consume()
                return "$"
            case "'":
                self._consume()
                return "'"
            case "`":
                self._consume()
                return "`"
            case "\\":
                self._consume()
                return "\\"

        # check for uxxxx
        if self._cur == "u":
            self._consume()

            try:
                n3 = int(self._cur, 16)
                self._consume()

                n2 = int(self._cur, 16)
                self._consume()

                n1 = int(self._cur, 16)
                self._consume()

                n0 = int(self._cur, 16)
                self._consume()

            except Exception:
                raise ValueError("Invalid hex value for \\uxxxx")

            return str((n3 << 12) | (n2 << 8) | (n1 << 4) | n0)

        raise ValueError("Invalid escape sequence")

    def _operator(self) -> HaystackToken:
        c = self._cur
        self._consume()

        match c:
            case ",":
                return HaystackToken.COMMA
            case ":":
                if self._cur == ":":
                    self._consume()
                    return HaystackToken.COLON2
                return HaystackToken.COLON
            case ";":
                return HaystackToken.SEMICOLON
            case "[":
                return HaystackToken.LBRACKET
            case "]":
                return HaystackToken.RBRACKET
            case "{":
                return HaystackToken.LBRACE
            case "}":
                return HaystackToken.RBRACE
            case "(":
                return HaystackToken.LPAREN
            case ")":
                return HaystackToken.RPAREN
            case "<":
                if self._cur == "<":
                    self._consume()
                    return HaystackToken.LT2
                if self._cur == "=":
                    self._consume()
                    return HaystackToken.LTEQ
                return HaystackToken.LT
            case ">":
                if self._cur == ">":
                    self._consume()
                    return HaystackToken.GT2
                if self._cur == "=":
                    self._consume()
                    return HaystackToken.GTEQ
                return HaystackToken.GT
            case "-":
                if self._cur == ">":
                    self._consume()
                    return HaystackToken.ARROW
                return HaystackToken.MINUS
            case "=":
                if self._cur == "=":
                    self._consume()
                    return HaystackToken.EQ
                if self._cur == ">":
                    self._consume()
                    return HaystackToken.FNARROW
                return HaystackToken.ASSIGN
            case "!":
                if self._cur == "=":
                    self._consume()
                    return HaystackToken.NOTEQ
                return HaystackToken.BANG
            case "/":
                return HaystackToken.SLASH
            case ".":
                return HaystackToken.DOT
            case "?":
                return HaystackToken.QUESTION
            case "&":
                return HaystackToken.AMP
            case "|":
                return HaystackToken.PIPE
            case "":
                return HaystackToken.EOF

        raise ValueError(f"Unexpected symbol: '{c}'")


class HaystackFactory:
    def make_id(self, s: str) -> str:
        return s

    def make_str(self, s: str) -> str:
        return s

    def make_uri(self, s: str) -> Uri:
        return Uri(s)

    def make_ref(self, val: str, dis: str | None = None) -> Ref:
        return Ref(val, dis)

    def make_symbol(self, s: str) -> Symbol:
        return Symbol(s)

    def make_time(self, s: str) -> time:
        return time.fromisoformat(s)

    def make_date(self, s: str) -> date:
        format = "%Y-%m-%d"
        return datetime.strptime(s, format).date()

    def make_datetime(self, s: str) -> datetime:
        if " " in s:
            dt_str, haystack_tz = s.split(" ")
            iana_tz = _haystack_to_iana_tz(haystack_tz)
            return datetime.fromisoformat(dt_str).replace(tzinfo=iana_tz)

        return datetime.fromisoformat(s)

    def make_number(self, val: float, unit: str | None) -> Number:
        return Number(val, unit)


class HaystackToken(StrEnum):
    # identifier/literals
    ID = "identifier"
    KEYWORD = "keyword"
    NUM = "Number"
    STR = "Str"
    REF = "Ref"
    SYMBOL = "Symbol"
    URI = "Uri"
    DATE = "Date"
    TIME = "Time"
    DATETIME = "DateTime"

    # operators
    DOT = "."
    COLON = ":"
    COLON2 = "::"
    COMMA = ","
    SEMICOLON = ";"
    MINUS = "-"
    EQ = "=="
    NOTEQ = "!="
    LT = "<"
    LT2 = "<<"
    LTEQ = "<="
    GT = ">"
    GT2 = ">>"
    GTEQ = ">="
    LBRACE = "{"
    RBRACE = "}"
    LPAREN = "("
    RPAREN = ")"
    LBRACKET = "["
    RBRACKET = "]"
    ARROW = "->"
    FNARROW = "=>"
    SLASH = "/"
    ASSIGN = "="
    BANG = "!"
    QUESTION = "?"
    AMP = "&"
    PIPE = "|"
    NL = "newline"

    # misc
    COMMENT = "comment"
    EOF = "eof"


def _is_ref_id_char(c: str) -> bool:
    return c.isalnum() or c in ["_", ":", "-", ".", "~"]
