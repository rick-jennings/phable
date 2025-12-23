from datetime import date, datetime, time
from decimal import Decimal
from math import isnan
from zoneinfo import ZoneInfo

import pytest

from phable.io.zinc_decoder import ZincDecoder
from phable.io.zinc_encoder import ZincEncoder
from phable.kinds import (
    NA,
    Coord,
    Grid,
    GridCol,
    Marker,
    Number,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
)


@pytest.mark.parametrize(
    "zinc,expected",
    [
        # 1x0
        (
            r"""ver:"3.0"
fooBar33

""",
            Grid({"ver": "3.0"}, [GridCol("fooBar33")], []),
        ),
        # 1x1
        (
            r"""ver:"3.0" tag foo:"bar"
xyz
"val"

""",
            Grid.to_grid({"xyz": "val"}, {"tag": Marker(), "foo": "bar"}),
        ),
        #         # 1x1 null
        #         (
        #             r"""ver:"3.0"
        # val
        # N
        # """,
        #             Grid.to_grid({"val": None}),
        #         ),
        # 2x2
        (
            r"""ver:"3.0"
a,b
1,2
3,4

""",
            Grid.to_grid(
                [{"a": Number(1), "b": Number(2)}, {"a": Number(3), "b": Number(4)}]
            ),
        ),
        # all scalars
        (
            r"""ver:"3.0"
a,b,c,d
T,F,,-99.0
2.3,-5e-10,2.4e+20,1.23e-08
"","a","\" \\ \t \n \r","\uabcd"
`path`,@12cbb082-0c02ae73,4s,-2.5min
M,R,,
2009-12-31,23:59:01,01:02:03.123000,2009-02-03T04:05:06Z
INF,-INF,"",
C(12,-34),C(0.123,-0.789),C(84.5,-77.45),C(-90,180)
NA,,^a:b,"foo"

""",
            Grid(
                {"ver": "3.0"},
                [GridCol("a"), GridCol("b"), GridCol("c"), GridCol("d")],
                [
                    {"a": True, "b": False, "d": Number(-99.0)},
                    {
                        "a": Number(2.3),
                        "b": Number(-5e-10),
                        "c": Number(2.4e20),
                        "d": Number(123e-10),
                    },
                    {"a": "", "b": "a", "c": '" \\ \t \n \r', "d": "\uabcd"},
                    {
                        "a": Uri("path"),
                        "b": Ref("12cbb082-0c02ae73"),
                        "c": Number(4, "s"),
                        "d": Number(-2.5, "min"),
                    },
                    {"a": Marker(), "b": Remove()},
                    {
                        "a": date(2009, 12, 31),
                        "b": time(23, 59, 1),
                        "c": time(1, 2, 3, 123_000),
                        "d": datetime(2009, 2, 3, 4, 5, 6, tzinfo=ZoneInfo("UTC")),
                    },
                    {
                        "a": Number(float("inf")),
                        "b": Number(float("-inf")),
                        "c": "",
                    },
                    {
                        "a": Coord(Decimal("12"), Decimal("-34")),
                        "b": Coord(Decimal("0.123"), Decimal("-0.789")),
                        "c": Coord(Decimal("84.5"), Decimal("-77.45")),
                        "d": Coord(Decimal("-90"), Decimal("180")),
                    },
                    {"a": NA(), "c": Symbol("a:b"), "d": "foo"},
                ],
            ),
        ),
        # specials
        (
            r"""ver:"3.0"
_foo
`foo$20bar`
`foo\`bar`
`file \#2`
"$15"

""",
            Grid.to_grid(
                [
                    {"_foo": Uri("foo$20bar")},
                    {"_foo": Uri("foo`bar")},
                    {"_foo": Uri("file \\#2")},
                    {"_foo": "$15"},
                ]
            ),
        ),
        # units
        (
            r"""ver:"3.0"
a,b
-3.1kg,4kg
5%,3.2%
5kWh/ft²,-15kWh/m²
123.12kW,74Δ°F

""",
            Grid.to_grid(
                [
                    {"a": Number(-3.1, "kg"), "b": Number(4, "kg")},
                    {"a": Number(5, "%"), "b": Number(3.2, "%")},
                    {"a": Number(5, "kWh/ft²"), "b": Number(-15, "kWh/m²")},
                    {"a": Number(123.12, "kW"), "b": Number(74, "Δ°F")},
                ]
            ),
        ),
        # xstr
        (
            r"""ver:"3.0"
a,b
Foo("foo"),C("")
,B("b\n)!")
Span("2016-01-10"),Color("#fff")

""",
            Grid.to_grid(
                [
                    {"a": XStr("Foo", "foo"), "b": XStr("C", "")},
                    {"b": XStr("B", "b\n)!")},
                    {"a": XStr("Span", "2016-01-10"), "b": XStr("Color", "#fff")},
                ]
            ),
        ),
        # sparse
        (
            #             r"""ver:"3.0"
            # a,_b,__45
            # ,1,2
            # 3,,5
            # 6,7000,
            # ,,10
            # ,,
            # 14,,
            # """,
            r"""ver:"3.0"
a,_b,__45
,1,2
3,,5
6,7000,
,,10
14,,

""",
            Grid(
                {"ver": "3.0"},
                [GridCol("a"), GridCol("_b"), GridCol("__45")],
                [
                    {"_b": Number(1), "__45": Number(2)},
                    {"a": Number(3), "__45": Number(5)},
                    {"a": Number(6), "_b": Number(7_000)},
                    {"__45": Number(10)},
                    {"a": Number(14)},
                ],
            ),
        ),
        # sparse
        (
            r"""ver:"3.0"
a,b
2010-03-01T23:55:00.013000-05:00 GMT+5,2010-03-01T23:55:00.013000+10:00 GMT-10

""",
            Grid.to_grid(
                {
                    "a": datetime(
                        2010, 3, 1, 23, 55, 0, 13_000, tzinfo=ZoneInfo("Etc/GMT+5")
                    ),
                    "b": datetime(
                        2010, 3, 1, 23, 55, 0, 13_000, tzinfo=ZoneInfo("Etc/GMT-10")
                    ),
                }
            ),
        ),
        # timezones and regression bugs
        (
            r"""ver:"3.0" a:2009-02-03T04:05:06Z foo b:2010-02-03T04:05:06-05:00 New_York bar c:2009-12-03T04:05:06Z baz
a
2010-12-18T14:11:30.924000Z
45$
33£
@12cbb08e-0c02ae73

""",
            Grid(
                {
                    "ver": "3.0",
                    "a": datetime(2009, 2, 3, 4, 5, 6, tzinfo=ZoneInfo("UTC")),
                    "foo": Marker(),
                    "b": datetime(
                        2010, 2, 3, 4, 5, 6, tzinfo=ZoneInfo("America/New_York")
                    ),
                    "bar": Marker(),
                    "c": datetime(2009, 12, 3, 4, 5, 6, tzinfo=ZoneInfo("UTC")),
                    "baz": Marker(),
                },
                [GridCol("a")],
                [
                    {
                        "a": datetime(
                            2010, 12, 18, 14, 11, 30, 924_000, tzinfo=ZoneInfo("UTC")
                        )
                    },
                    {"a": Number(45, "$")},
                    {"a": Number(33, "£")},
                    {"a": Ref("12cbb08e-0c02ae73")},
                ],
            ),
        ),
    ],
)
def test_parse_zinc(zinc: str, expected: Grid):
    zinc_decoded = ZincDecoder().from_str(zinc)

    assert isinstance(zinc_decoded, Grid)
    assert zinc_decoded == expected
    assert ZincEncoder().to_str(expected) == zinc


def test_parse_nan():
    x = r"{a: NaN, b: -2.5min}"

    zinc_decoded = ZincDecoder().from_str(x)

    assert isinstance(zinc_decoded, dict)
    assert isinstance(zinc_decoded["a"], Number)
    assert isnan(zinc_decoded["a"].val)
    assert zinc_decoded["b"] == Number(-2.5, "min")


def test_parse_scientific_notation():
    x = r"{test1: 123e+12kJ/kg_dry, test2: 7.15625E-4kWh/ft², test3: 3.814697265625E-6}"

    zinc_decoded = ZincDecoder().from_str(x)

    assert isinstance(zinc_decoded, dict)
    assert zinc_decoded["test1"] == Number(123e12, "kJ/kg_dry")
    assert zinc_decoded["test2"] == Number(7.15625e-4, "kWh/ft²")
    assert zinc_decoded["test3"] == Number(3.814697265625e-6)


def test_parse_datetime():
    x = r"{x: 2010-12-18T14:11:30.925000Z UTC, y: 2010-12-18T14:11:30.925000Z London, z: 2015-01-02T06:13:38.701000-08:00 PST8PDT}"
    zinc_decoded = ZincDecoder().from_str(x)

    expected = {
        "x": datetime(2010, 12, 18, 14, 11, 30, 925_000, tzinfo=ZoneInfo("UTC")),
        "y": datetime(
            2010,
            12,
            18,
            14,
            11,
            30,
            925_000,
            tzinfo=ZoneInfo("Europe/London"),
        ),
        "z": datetime(
            2015,
            1,
            2,
            6,
            13,
            38,
            701_000,
            tzinfo=ZoneInfo("PST8PDT"),
        ),
    }

    assert zinc_decoded == expected


def test_number_with_underscore():
    x = r"{test: 7_000}"

    zinc_decoded = ZincDecoder().from_str(x)

    assert isinstance(zinc_decoded, dict)
    assert zinc_decoded["test"] == Number(7_000)


@pytest.mark.parametrize(
    "zinc,expected",
    [
        # simple one grid
        (
            r"""ver:"3.0"
val
<<
ver:"3.0"
x,y
4,6

>>
"foo"

""",
            Grid.to_grid(
                [
                    {"val": Grid.to_grid({"x": Number(4), "y": Number(6)})},
                    {"val": "foo"},
                ]
            ),
        ),
        # one col, two rows of grids
        (
            r"""ver:"3.0"
val
<<
ver:"3.0"
x,y
4,6

>>
<<
ver:"3.0" foo
z
1
2

>>

""",
            Grid.to_grid(
                [
                    {"val": Grid.to_grid({"x": Number(4), "y": Number(6)})},
                    {
                        "val": Grid.to_grid(
                            [{"z": Number(1)}, {"z": Number(2)}],
                            {"foo": Marker()},
                        )
                    },
                ],
            ),
        ),
        # two cols of grids
        (
            r"""ver:"3.0"
col1,col2
<<
ver:"3.0"
x,y
4,6

>>,<<
ver:"3.0" foo
z
1
2

>>

""",
            Grid.to_grid(
                {
                    "col1": Grid.to_grid([{"x": Number(4), "y": Number(6)}]),
                    "col2": Grid.to_grid(
                        [{"z": Number(1)}, {"z": Number(2)}], {"foo": Marker()}
                    ),
                }
            ),
        ),
        # 3x2 of grids
        (
            r"""ver:"3.0"
col1,col2,col3
<<
ver:"3.0"
a
1

>>,<<
ver:"3.0"
b
1

>>,<<
ver:"3.0"
c
1

>>
<<
ver:"3.0"
a
2

>>,<<
ver:"3.0"
b
2

>>,<<
ver:"3.0"
c
2

>>

""",
            Grid.to_grid(
                [
                    {
                        "col1": Grid.to_grid({"a": Number(1)}),
                        "col2": Grid.to_grid({"b": Number(1)}),
                        "col3": Grid.to_grid({"c": Number(1)}),
                    },
                    {
                        "col1": Grid.to_grid({"a": Number(2)}),
                        "col2": Grid.to_grid({"b": Number(2)}),
                        "col3": Grid.to_grid({"c": Number(2)}),
                    },
                ]
            ),
        ),
        # double nesting
        (
            r"""ver:"3.0"
outer
<<
ver:"3.0"
inner
<<
ver:"3.0"
x
1

>>
<<
ver:"3.0"
y
2

>>

>>

""",
            Grid.to_grid(
                {
                    "outer": Grid.to_grid(
                        [
                            {"inner": Grid.to_grid({"x": Number(1)})},
                            {"inner": Grid.to_grid({"y": Number(2)})},
                        ]
                    )
                }
            ),
        ),
    ],
)
def test_nested(zinc: str, expected: Grid):
    zinc_decoded = ZincDecoder().from_str(zinc)

    assert zinc_decoded == expected
    assert ZincEncoder().to_str(expected) == zinc


@pytest.mark.parametrize(
    "zinc,expected_msg",
    [
        (
            r"""ver:"3.0"
            foo
            @
            """,
            "Invalid empty Ref",
        ),
        (
            r"""ver:"3.0"
            foo
            @@
            """,
            "Invalid empty Ref",
        ),
    ],
)
def test_exceptions(zinc: str, expected_msg: str):
    with pytest.raises(ValueError) as e:
        ZincDecoder().from_str(zinc)
    assert str(e.value) == expected_msg


def test_refs():
    zinc = r"""ver:"3.0" siteRef:@17eb894a-26bb44ff "HQ" mark
id,ref childRef:@17eb894a-26bb44dd "Child" parentRef:@17eb894a-26bb44ee "Parent"
@17eb894a-26bb4400,@17eb894a-26bb440a
@17eb894a-26bb4401 "Alpha",@17eb894a-26bb440b "Beta"

"""

    zinc_decoded = ZincDecoder().from_str(zinc)

    expected = Grid(
        {"ver": "3.0", "siteRef": Ref("17eb894a-26bb44ff", "HQ"), "mark": Marker()},
        [
            GridCol("id"),
            GridCol(
                "ref",
                {
                    "childRef": Ref("17eb894a-26bb44dd", "Child"),
                    "parentRef": Ref("17eb894a-26bb44ee", "Parent"),
                },
            ),
        ],
        [
            {"id": Ref("17eb894a-26bb4400"), "ref": Ref("17eb894a-26bb440a")},
            {
                "id": Ref("17eb894a-26bb4401", "Alpha"),
                "ref": Ref("17eb894a-26bb440b", "Beta"),
            },
        ],
    )

    assert zinc_decoded == expected
    assert ZincEncoder().to_str(expected) == zinc


# @pytest.mark.parametrize(
#     "zinc,expected_msg",
#     [
#         ("", {}),
#         ("foo", {"foo": Marker()}),
#         ("age:12", {"age": Number(12)}),
#         ("age:12yr", {"age": Number(12, "yr")}),
#         ('name:"b" bday:1972-06-07', {"name": "b", "bday": date(1972, 6, 7)}),
#         (
#             'name:"b" bday:1972-06-07 cool',
#             {"name": "b", "bday": date(1972, 6, 7), "cool": Marker()},
#         ),
#         (
#             "foo: 1, bar: 2 baz: 3",
#             {"foo": Number(1), "bar": Number(2), "baz": Number(3)},
#         ),  # commas
#         (
#             "foo: 1, bar: 2, baz: 3",
#             {"foo": Number(1), "bar": Number(2), "baz": Number(3)},
#         ),  # commas
#     ],
# )
# def test_tags(zinc: str, expected_msg: dict[str, Any]):
#     zinc_decoded = ZincDecoder.read_tags(zinc)
#     assert zinc_decoded == expected_msg


def test_list():
    raw_zinc = '{x:[1,[2,3,4,"abc"],`def`,5]}'

    zinc_decoded = ZincDecoder().from_str(raw_zinc)

    expected = {
        "x": [
            Number(1),
            [Number(2), Number(3), Number(4), "abc"],
            Uri("def"),
            Number(5),
        ]
    }

    assert zinc_decoded == expected
    assert ZincEncoder().to_str(expected) == raw_zinc
