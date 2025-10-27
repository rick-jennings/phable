from datetime import date, datetime, time
from io import StringIO
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from phable.io.ph_tokenizer import PhToken, PhTokenizer
from phable.kinds import Number, Ref, Uri


@pytest.mark.parametrize(
    "x,expected",
    [
        # empty
        ("", []),
        # symbols
        ("!", [(PhToken.BANG, None)]),
        ("?", [(PhToken.QUESTION, None)]),
        (
            "= => ==",
            [
                (PhToken.ASSIGN, None),
                (PhToken.FNARROW, None),
                (PhToken.EQ, None),
            ],
        ),
        (
            "- ->",
            [
                (PhToken.MINUS, None),
                (PhToken.ARROW, None),
            ],
        ),
        # identifiers
        ("x", [(PhToken.ID, "x")]),
        ("fooBar", [(PhToken.ID, "fooBar")]),
        ("fooBar1999x", [(PhToken.ID, "fooBar1999x")]),
        ("foo_23", [(PhToken.ID, "foo_23")]),
        ("Foo", [(PhToken.ID, "Foo")]),
        ("_3", [(PhToken.ID, "_3")]),
        ("__90", [(PhToken.ID, "__90")]),
        # ints
        ("5", [(PhToken.NUM, Number(5))]),
        ("0x1234_abcd", [(PhToken.NUM, Number(0x1234_ABCD))]),
        # floats
        ("5.0", [(PhToken.NUM, Number(5.0))]),
        ("5.42", [(PhToken.NUM, Number(5.42))]),
        ("123.2e32", [(PhToken.NUM, Number(123.2e32))]),
        ("123.2e+32", [(PhToken.NUM, Number(123.2e32))]),
        ("2_123.2e+32", [(PhToken.NUM, Number(2_123.2e32))]),
        ("4.2e-7", [(PhToken.NUM, Number(4.2e-7))]),
        # numbers with units
        ("-40ms", [(PhToken.NUM, Number(-40, "ms"))]),
        ("1sec", [(PhToken.NUM, Number(1, "sec"))]),
        ("5hr", [(PhToken.NUM, Number(5, "hr"))]),
        ("2.5day", [(PhToken.NUM, Number(2.5, "day"))]),
        ("12%", [(PhToken.NUM, Number(12, "%"))]),
        ("987_foo", [(PhToken.NUM, Number(987, "_foo"))]),
        ("-1.2m/s", [(PhToken.NUM, Number(-1.2, "m/s"))]),
        (
            "12kWh/ft\u00b2",
            [(PhToken.NUM, Number(12, "kWh/ft\u00b2"))],
        ),
        (
            "3_000.5J/kg_dry",
            [(PhToken.NUM, Number(3_000.5, "J/kg_dry"))],
        ),
        # strings
        ('""', [(PhToken.STR, "")]),
        ('"x y"', [(PhToken.STR, "x y")]),
        ('"x\\"y"', [(PhToken.STR, 'x"y')]),
        (
            '"_\u012f \n \t \\\\_ \u1f973"',
            [(PhToken.STR, "_\u012f \n \t \\_ \u1f973")],
        ),
        # date
        ("2009-10-04", [(PhToken.DATE, date(2009, 10, 4))]),
        # time
        ("8:30", [(PhToken.TIME, time(8, 30))]),
        ("20:15", [(PhToken.TIME, time(20, 15))]),
        ("00:00", [(PhToken.TIME, time(0, 0))]),
        ("01:02:03", [(PhToken.TIME, time(1, 2, 3))]),
        ("23:59:59", [(PhToken.TIME, time(23, 59, 59))]),
        ("12:00:12.345", [(PhToken.TIME, time(12, 0, 12, 345_000))]),
        # date time
        (
            "2016-01-13T09:51:33-05:00 New_York",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2016, 1, 13, 9, 51, 33, tzinfo=ZoneInfo("America/New_York")
                    ),
                )
            ],
        ),
        (
            "2016-01-13T09:51:33.353-05:00 New_York",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2016,
                        1,
                        13,
                        9,
                        51,
                        33,
                        353_000,
                        tzinfo=ZoneInfo("America/New_York"),
                    ),
                )
            ],
        ),
        (
            "2010-12-18T14:11:30.924Z",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        12,
                        18,
                        14,
                        11,
                        30,
                        924_000,
                        tzinfo=ZoneInfo("UTC"),
                    ),
                )
            ],
        ),
        (
            "2010-12-18T14:11:30.925Z UTC",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        12,
                        18,
                        14,
                        11,
                        30,
                        925_000,
                        tzinfo=ZoneInfo("UTC"),
                    ),
                )
            ],
        ),
        (
            "2010-12-18T14:11:30.925Z London",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        12,
                        18,
                        14,
                        11,
                        30,
                        925_000,
                        tzinfo=ZoneInfo("Europe/London"),
                    ),
                )
            ],
        ),
        (
            "2015-01-02T06:13:38.701-08:00 PST8PDT",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2015,
                        1,
                        2,
                        6,
                        13,
                        38,
                        701_000,
                        tzinfo=ZoneInfo("PST8PDT"),
                    ),
                )
            ],
        ),
        (
            "2010-03-01T23:55:00.013-05:00 GMT+5",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        3,
                        1,
                        23,
                        55,
                        00,
                        13_000,
                        tzinfo=ZoneInfo("Etc/GMT+5"),
                    ),
                )
            ],
        ),
        (
            "2010-03-01T23:55:00.013+10:00 GMT-10",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        3,
                        1,
                        23,
                        55,
                        00,
                        13_000,
                        tzinfo=ZoneInfo("Etc/GMT-10"),
                    ),
                )
            ],
        ),
        (
            "2010-03-01T23:55:00.013+10:00 Port-au-Prince",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        3,
                        1,
                        23,
                        55,
                        00,
                        13_000,
                        tzinfo=ZoneInfo("America/Port-au-Prince"),
                    ),
                )
            ],
        ),
        # date time + dot
        (
            "2016-01-13T09:51:33.353-05:00 New_York.",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2016,
                        1,
                        13,
                        9,
                        51,
                        33,
                        353_000,
                        tzinfo=ZoneInfo("America/New_York"),
                    ),
                ),
                (PhToken.DOT, None),
            ],
        ),
        (
            "2010-03-01T23:55:00.013-05:00 GMT+5.",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        3,
                        1,
                        23,
                        55,
                        00,
                        13_000,
                        tzinfo=ZoneInfo("Etc/GMT+5"),
                    ),
                ),
                (PhToken.DOT, None),
            ],
        ),
        (
            "2010-03-01T23:55:00.013+10:00 Port-au-Prince.",
            [
                (
                    PhToken.DATETIME,
                    datetime(
                        2010,
                        3,
                        1,
                        23,
                        55,
                        00,
                        13_000,
                        tzinfo=ZoneInfo("Etc/GMT+5"),
                    ),
                ),
                (PhToken.DOT, None),
            ],
        ),
        # uri
        ("`http://foo/`", [(PhToken.URI, Uri("http://foo/"))]),
        ("`_ \\n \\\\ \\`_`", [(PhToken.URI, Uri("_ \n \\\\ `_"))]),
        # Ref
        ("@125b780e-0684e169", [(PhToken.REF, Ref("125b780e-0684e169"))]),
        ("@demo:_:-.~", [(PhToken.REF, Ref("demo:_:-.~"))]),
        # newlines and whitespace
        (
            "a\n  b  \rc \r\nd\n\ne",
            [
                (PhToken.ID, "a"),
                (PhToken.NL, None),
                (PhToken.ID, "b"),
                (PhToken.NL, None),
                (PhToken.ID, "c"),
                (PhToken.NL, None),
                (PhToken.ID, "d"),
                (PhToken.NL, None),
                (PhToken.NL, None),
                (PhToken.ID, "e"),
            ],
        ),
        # comments
        (
            """// foo
//   bar
x  // baz
""",
            [
                (PhToken.NL, None),
                (PhToken.NL, None),
                (PhToken.ID, "x"),
                (PhToken.NL, None),
            ],
        ),
    ],
)
def test_haystack_tokenizer(
    x: str,
    expected: list[tuple[PhToken, str]],
):
    t = PhTokenizer(StringIO(x))
    verify_toks(t, expected)


@pytest.mark.parametrize(
    "x,keywords,expected",
    [
        # keywords
        ("x", {"x": "x"}, [(PhToken.KEYWORD, "x")]),
        ("x", {"x": "_x_"}, [(PhToken.KEYWORD, "_x_")]),
    ],
)
def test_haystack_tokenizer_with_keywords(
    x: str,
    keywords: dict[str, Any] | None,
    expected: list[tuple[PhToken, str]],
):
    t = PhTokenizer(StringIO(x))
    t.keywords = keywords.copy()
    verify_toks(t, expected)


@pytest.mark.parametrize(
    "x,expected",
    [
        # comments
        (
            """// foo
//   bar
x  // baz
""",
            [
                (PhToken.COMMENT, "foo"),
                (PhToken.NL, None),
                (PhToken.COMMENT, "  bar"),
                (PhToken.NL, None),
                (PhToken.ID, "x"),
                (PhToken.COMMENT, "baz"),
                (PhToken.NL, None),
            ],
        ),
    ],
)
def test_haystack_tokenizer_with_keep_comments(
    x: str,
    expected: list[tuple[PhToken, str]],
):
    t = PhTokenizer(StringIO(x))
    t.keep_comments = True
    verify_toks(t, expected)


@pytest.mark.parametrize(
    "x,expected,line",
    [
        ('"fo..', "Unexpected end of str", 1),
        ("`fo..", "Unexpected end of uri", 1),
        ('"\\u345x"', "Invalid hex value for \\u345x", 1),
        ('"\\ua"', "Invalid hex value for \\ua", 1),
        ('"\\u234"', "Invalid hex value for \\u234", 1),
        ("#", "Unexpected symbol: '#'", 1),
        ("\n\n#", "Unexpected symbol: '#'", 3),
    ],
)
def test_haystack_tokenizer_raises_error(
    x: str, expected: list[tuple[PhToken, str]], line: int
):
    t = PhTokenizer(StringIO(x))
    with pytest.raises(ValueError) as e:
        verify_toks(t, expected)

    assert str(e.value) == expected
    assert t.line == line


def verify_toks(t: PhTokenizer, expected: Any) -> None:
    acc = []

    while True:
        x = t.next()

        if x == PhToken.EOF:
            break

        acc.append((t.tok, t.val))

    assert acc == expected
