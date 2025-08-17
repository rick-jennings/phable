from datetime import date, datetime, time
from io import StringIO
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from phable.kinds import Number, Ref, Uri
from phable.parsers.haystack_tokenizer import HaystackToken, HaystackTokenizer


@pytest.mark.parametrize(
    "x,expected",
    [
        # empty
        ("", []),
        # symbols
        ("!", [(HaystackToken.BANG, None)]),
        ("?", [(HaystackToken.QUESTION, None)]),
        (
            "= => ==",
            [
                (HaystackToken.ASSIGN, None),
                (HaystackToken.FNARROW, None),
                (HaystackToken.EQ, None),
            ],
        ),
        (
            "- ->",
            [
                (HaystackToken.MINUS, None),
                (HaystackToken.ARROW, None),
            ],
        ),
        # identifiers
        ("x", [(HaystackToken.ID, "x")]),
        ("fooBar", [(HaystackToken.ID, "fooBar")]),
        ("fooBar1999x", [(HaystackToken.ID, "fooBar1999x")]),
        ("foo_23", [(HaystackToken.ID, "foo_23")]),
        ("Foo", [(HaystackToken.ID, "Foo")]),
        ("_3", [(HaystackToken.ID, "_3")]),
        ("__90", [(HaystackToken.ID, "__90")]),
        # ints
        ("5", [(HaystackToken.NUM, Number(5))]),
        ("0x1234_abcd", [(HaystackToken.NUM, Number(0x1234_ABCD))]),
        # floats
        ("5.0", [(HaystackToken.NUM, Number(5.0))]),
        ("5.42", [(HaystackToken.NUM, Number(5.42))]),
        ("123.2e32", [(HaystackToken.NUM, Number(123.2e32))]),
        ("123.2e+32", [(HaystackToken.NUM, Number(123.2e32))]),
        ("2_123.2e+32", [(HaystackToken.NUM, Number(2_123.2e32))]),
        ("4.2e-7", [(HaystackToken.NUM, Number(4.2e-7))]),
        # numbers with units
        ("-40ms", [(HaystackToken.NUM, Number(-40, "ms"))]),
        ("1sec", [(HaystackToken.NUM, Number(1, "sec"))]),
        ("5hr", [(HaystackToken.NUM, Number(5, "hr"))]),
        ("2.5day", [(HaystackToken.NUM, Number(2.5, "day"))]),
        ("12%", [(HaystackToken.NUM, Number(12, "%"))]),
        ("987_foo", [(HaystackToken.NUM, Number(987, "_foo"))]),
        ("-1.2m/s", [(HaystackToken.NUM, Number(-1.2, "m/s"))]),
        (
            "12kWh/ft\u00b2",
            [(HaystackToken.NUM, Number(12, "kWh/ft\u00b2"))],
        ),
        (
            "3_000.5J/kg_dry",
            [(HaystackToken.NUM, Number(3_000.5, "J/kg_dry"))],
        ),
        # strings
        ('""', [(HaystackToken.STR, "")]),
        ('"x y"', [(HaystackToken.STR, "x y")]),
        ('"x\\"y"', [(HaystackToken.STR, 'x"y')]),
        (
            '"_\u012f \n \t \\\\_ \u1f973"',
            [(HaystackToken.STR, "_\u012f \n \t \\_ \u1f973")],
        ),
        # date
        ("2009-10-04", [(HaystackToken.DATE, date(2009, 10, 4))]),
        # time
        ("8:30", [(HaystackToken.TIME, time(8, 30))]),
        ("20:15", [(HaystackToken.TIME, time(20, 15))]),
        ("00:00", [(HaystackToken.TIME, time(0, 0))]),
        ("01:02:03", [(HaystackToken.TIME, time(1, 2, 3))]),
        ("23:59:59", [(HaystackToken.TIME, time(23, 59, 59))]),
        ("12:00:12.345", [(HaystackToken.TIME, time(12, 0, 12, 345_000))]),
        # date time
        (
            "2016-01-13T09:51:33-05:00 New_York",
            [
                (
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                    HaystackToken.DATETIME,
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
                (HaystackToken.DOT, None),
            ],
        ),
        (
            "2010-03-01T23:55:00.013-05:00 GMT+5.",
            [
                (
                    HaystackToken.DATETIME,
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
                (HaystackToken.DOT, None),
            ],
        ),
        (
            "2010-03-01T23:55:00.013+10:00 Port-au-Prince.",
            [
                (
                    HaystackToken.DATETIME,
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
                (HaystackToken.DOT, None),
            ],
        ),
        # uri
        ("`http://foo/`", [(HaystackToken.URI, Uri("http://foo/"))]),
        ("`_ \\n \\\\ \\`_`", [(HaystackToken.URI, Uri("_ \n \\\\ `_"))]),
        # Ref
        ("@125b780e-0684e169", [(HaystackToken.REF, Ref("125b780e-0684e169"))]),
        ("@demo:_:-.~", [(HaystackToken.REF, Ref("demo:_:-.~"))]),
        # newlines and whitespace
        (
            "a\n  b  \rc \r\nd\n\ne",
            [
                (HaystackToken.ID, "a"),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "b"),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "c"),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "d"),
                (HaystackToken.NL, None),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "e"),
            ],
        ),
        # comments
        (
            """// foo
//   bar
x  // baz
""",
            [
                (HaystackToken.NL, None),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "x"),
                (HaystackToken.NL, None),
            ],
        ),
    ],
)
def test_haystack_tokenizer(
    x: str,
    expected: list[tuple[HaystackToken, str]],
):
    t = HaystackTokenizer(StringIO(x))
    verify_toks(t, expected)


@pytest.mark.parametrize(
    "x,keywords,expected",
    [
        # keywords
        ("x", {"x": "x"}, [(HaystackToken.KEYWORD, "x")]),
        ("x", {"x": "_x_"}, [(HaystackToken.KEYWORD, "_x_")]),
    ],
)
def test_haystack_tokenizer_with_keywords(
    x: str,
    keywords: dict[str, Any] | None,
    expected: list[tuple[HaystackToken, str]],
):
    t = HaystackTokenizer(StringIO(x))
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
                (HaystackToken.COMMENT, "foo"),
                (HaystackToken.NL, None),
                (HaystackToken.COMMENT, "  bar"),
                (HaystackToken.NL, None),
                (HaystackToken.ID, "x"),
                (HaystackToken.COMMENT, "baz"),
                (HaystackToken.NL, None),
            ],
        ),
    ],
)
def test_haystack_tokenizer_with_keep_comments(
    x: str,
    expected: list[tuple[HaystackToken, str]],
):
    t = HaystackTokenizer(StringIO(x))
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
    x: str, expected: list[tuple[HaystackToken, str]], line: int
):
    t = HaystackTokenizer(StringIO(x))
    with pytest.raises(ValueError) as e:
        verify_toks(t, expected)

    assert str(e.value) == expected
    assert t.line == line


def verify_toks(t: HaystackTokenizer, expected: Any) -> None:
    acc = []

    while True:
        x = t.next()

        if x == HaystackToken.EOF:
            break

        acc.append((t.tok, t.val))

    assert acc == expected
