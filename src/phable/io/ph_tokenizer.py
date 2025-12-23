from __future__ import annotations

from datetime import date, datetime, time
from enum import StrEnum
from typing import Any, TextIO

from phable.io.json_decoder import _haystack_to_iana_tz
from phable.kinds import Number, Ref, Symbol, Uri


class PhTokenizer:
    def __init__(self, input: TextIO):
        self._input = input
        self.tok = PhToken.EOF
        self.factory = HaystackFactory()
        self._consume()
        self._consume()

    _input: TextIO
    _cur: str
    _peek: str | None = None
    factory: HaystackFactory

    # Current token type
    tok: PhToken

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

    def next(self) -> PhToken:
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
            self.tok = PhToken.NL
            return self.tok

        assert isinstance(self._peek, str)

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

    def close(self) -> None:
        self._input.close()

    # Parse single line comment when keeping comments
    def parse_comment(self) -> PhToken:
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
        return PhToken.COMMENT

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
        self._cur = self._peek  # ty: ignore [invalid-assignment]
        self._peek = self._input.read(1)

    def _id(self) -> PhToken:
        s = ""
        while self._cur.isalnum() or self._cur == "_":
            s += self._cur
            self._consume()

        id = self.factory.make_id(s)

        # check for keyword
        if self.keywords is not None and self.keywords.get(id) is not None:
            self.val = self.keywords[id]
            return PhToken.KEYWORD

        # normal id
        self.val = id
        return PhToken.ID

    def _str(self) -> PhToken:
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

        self.val = self.factory.make_str(s)
        return PhToken.STR

    def _num(self) -> PhToken:
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
            return PhToken.NUM

        # consume all the things that might be part of this number token
        s = self._cur
        self._consume()
        colons, dashes, unit_index, exp = 0, 0, 0, False

        while True:
            assert isinstance(self._peek, str)
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
                self.val = self.factory.make_date(s)
            except Exception:
                raise ValueError(f"Invalid Date literal '{s}'")
            return PhToken.DATE

        # Time: we don't require hour to be two digits and
        # we don't require seconds
        if dashes == 0 and colons >= 1:
            if s[1] == ":":
                s = "0" + s
            if colons == 1:
                s = s + ":00"
            try:
                self.val = self.factory.make_time(s)
            except Exception:
                raise ValueError(f"Invalid Time literal '{s}'")

            return PhToken.TIME

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
                self.val = self.factory.make_datetime(s)
            except Exception:
                raise ValueError(f"Invalid DateTime literal '{s}'")

            return PhToken.DATETIME

        # parse as number
        if unit_index == 0:
            try:
                x = float(s)
            except Exception:
                raise ValueError(f"Invalid Number literal '{s}'")
            self.val = self.factory.make_number(x, None)
        else:
            float_str = s[0:unit_index]
            unit_str = s[unit_index::]

            try:
                x = float(float_str)
            except Exception:
                raise ValueError(f"Invalid Number literal '{s}'")

            self.val = self.factory.make_number(x, unit_str)

        return PhToken.NUM

    def _ref(self) -> PhToken:
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

        self.val = self.factory.make_ref(s)
        return PhToken.REF

    def _symbol(self) -> PhToken:
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

        self.val = self.factory.make_symbol(s)
        return PhToken.SYMBOL

    def _uri(self) -> PhToken:
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
        self.val = self.factory.make_uri(s)
        return PhToken.URI

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
                x = self._cur

                self._consume()
                if self._cur == '"':
                    raise Exception

                x += self._cur
                self._consume()
                if self._cur == '"':
                    raise Exception

                x += self._cur
                self._consume()
                if self._cur == '"':
                    raise Exception

                x += self._cur
                self._consume()

                return chr(int(x, 16))

            except Exception:
                raise ValueError(f"Invalid hex value for \\u{x}")

        raise ValueError("Invalid escape sequence")

    def _operator(self) -> PhToken:
        c = self._cur
        self._consume()

        match c:
            case ",":
                return PhToken.COMMA
            case ":":
                if self._cur == ":":
                    self._consume()
                    return PhToken.COLON2
                return PhToken.COLON
            case ";":
                return PhToken.SEMICOLON
            case "[":
                return PhToken.LBRACKET
            case "]":
                return PhToken.RBRACKET
            case "{":
                return PhToken.LBRACE
            case "}":
                return PhToken.RBRACE
            case "(":
                return PhToken.LPAREN
            case ")":
                return PhToken.RPAREN
            case "<":
                if self._cur == "<":
                    self._consume()
                    return PhToken.LT2
                if self._cur == "=":
                    self._consume()
                    return PhToken.LTEQ
                return PhToken.LT
            case ">":
                if self._cur == ">":
                    self._consume()
                    return PhToken.GT2
                if self._cur == "=":
                    self._consume()
                    return PhToken.GTEQ
                return PhToken.GT
            case "-":
                if self._cur == ">":
                    self._consume()
                    return PhToken.ARROW
                return PhToken.MINUS
            case "=":
                if self._cur == "=":
                    self._consume()
                    return PhToken.EQ
                if self._cur == ">":
                    self._consume()
                    return PhToken.FNARROW
                return PhToken.ASSIGN
            case "!":
                if self._cur == "=":
                    self._consume()
                    return PhToken.NOTEQ
                return PhToken.BANG
            case "/":
                return PhToken.SLASH
            case ".":
                return PhToken.DOT
            case "?":
                return PhToken.QUESTION
            case "&":
                return PhToken.AMP
            case "|":
                return PhToken.PIPE
            case "":
                return PhToken.EOF

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


class PhToken(StrEnum):
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


def is_literal(token: PhToken) -> bool:
    match token:
        case (
            PhToken.NUM
            | PhToken.STR
            | PhToken.REF
            | PhToken.SYMBOL
            | PhToken.URI
            | PhToken.DATE
            | PhToken.TIME
            | PhToken.DATETIME
        ):
            return True
        case _:
            return False


def _is_ref_id_char(c: str) -> bool:
    return c.isalnum() or c in ["_", ":", "-", ".", "~"]
