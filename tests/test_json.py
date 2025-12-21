from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import phable.kinds as kinds
from phable.io.json_decoder import (
    JsonDecoder,
    _haystack_to_iana_tz,
)
from phable.io.json_encoder import JsonEncoder

# -----------------------------------------------------------------------------
# To JSON - tests for Kind to JSON
# -----------------------------------------------------------------------------


def test_datetime_to_json():
    now = datetime.now(ZoneInfo("America/New_York"))
    assert JsonEncoder.to_dict(now) == {
        "_kind": "dateTime",
        "val": now.isoformat(),
        "tz": "New_York",
    }


def test_date_to_json():
    today = date(2024, 3, 27)
    assert JsonEncoder.to_dict(today) == {"_kind": "date", "val": "2024-03-27"}


def test_time_to_json():
    t1 = time(12, 12, 59)
    assert JsonEncoder.to_dict(t1) == {"_kind": "time", "val": "12:12:59"}


def test_number_to_json():
    x = kinds.Number(20, "kW")
    assert JsonEncoder.to_dict(x) == {"_kind": "number", "val": 20, "unit": "kW"}

    y = kinds.Number(20)
    assert JsonEncoder.to_dict(y) == 20


def test_int_to_json():
    x = 24
    assert JsonEncoder.to_dict(x) == 24


def test_float_to_json():
    x = 24.1
    assert JsonEncoder.to_dict(x) == 24.1


def test_str_to_json():
    x = "Hello World!"
    assert JsonEncoder.to_dict(x) == x


def test_bool_to_json():
    x = True
    assert JsonEncoder.to_dict(x) == x


def test_ref_to_json():
    ref_id = "abc1234"
    assert JsonEncoder.to_dict(kinds.Ref(ref_id)) == {"_kind": "ref", "val": ref_id}

    assert JsonEncoder.to_dict(kinds.Ref(ref_id, "Carytown")) == {
        "_kind": "ref",
        "val": ref_id,
        "dis": "Carytown",
    }


def test_symbol_to_json():
    x = kinds.Symbol("abc")
    assert JsonEncoder.to_dict(x) == {"_kind": "symbol", "val": "abc"}


def test_marker_to_json():
    x = kinds.Marker()
    assert JsonEncoder.to_dict(x) == {"_kind": "marker"}


def test_na_to_json():
    x = kinds.NA()
    assert JsonEncoder.to_dict(x) == {"_kind": "na"}


def test_remove_to_json():
    x = kinds.Remove()
    assert JsonEncoder.to_dict(x) == {"_kind": "remove"}


def test_uri_to_json():
    x = kinds.Uri("https://project-haystack.org")
    assert JsonEncoder.to_dict(x) == {
        "_kind": "uri",
        "val": "https://project-haystack.org",
    }


def test_coord_to_json():
    x = kinds.Coord(Decimal("37.548266"), Decimal("-77.4491888"))
    assert JsonEncoder.to_dict(x) == {
        "_kind": "coord",
        "lat": 37.548266,
        "lng": -77.4491888,
    }


def test_xstr_to_json():
    x = kinds.XStr("value", "red")
    assert JsonEncoder.to_dict(x) == {"_kind": "xstr", "type": "value", "val": "red"}


@pytest.mark.parametrize(
    "col,expected",
    [
        (
            kinds.GridCol("temp"),
            {"name": "temp"},
        ),
        (
            kinds.GridCol("temp", {"unit": "°F", "dis": "Temperature"}),
            {"name": "temp", "meta": {"unit": "°F", "dis": "Temperature"}},
        ),
        (
            kinds.GridCol(
                "temp", {"id": kinds.Ref("sensor123"), "point": kinds.Marker()}
            ),
            {
                "name": "temp",
                "meta": {
                    "id": {"_kind": "ref", "val": "sensor123"},
                    "point": {"_kind": "marker"},
                },
            },
        ),
    ],
)
def test_grid_col_to_json(col: kinds.GridCol, expected: dict[str, Any]) -> None:
    assert JsonEncoder.to_dict(col) == expected


def test_list_to_json():
    x = [
        kinds.Number(12, "kW"),
        True,
        {"test": kinds.Marker()},
        kinds.Grid.to_grid({"id": kinds.Ref("test1"), "dis": "test1"}),
    ]
    assert JsonEncoder.to_dict(x) == [
        {"_kind": "number", "val": 12, "unit": "kW"},
        True,
        {"test": {"_kind": "marker"}},
        {
            "_kind": "grid",
            "meta": {"ver": "3.0"},
            "cols": [{"name": "id"}, {"name": "dis"}],
            "rows": [{"id": {"_kind": "ref", "val": "test1"}, "dis": "test1"}],
        },
    ]


def test_kind_to_json_raises_error():
    with pytest.raises(ValueError):
        JsonEncoder.to_dict(timedelta(days=5))


# -----------------------------------------------------------------------------
# To JSON - Dictionaries and Grids
# -----------------------------------------------------------------------------


def test__parse_dict_with_kinds_to_json():
    x = {"test_meta": kinds.Marker()}

    assert JsonEncoder.to_dict(x) == {"test_meta": {"_kind": "marker"}}


def test__parse_nested_dict_with_kinds_to_json1():
    x = {
        "x1": kinds.Marker(),
        "x2": {"y1": kinds.Marker(), "id": kinds.Ref("y1")},
    }

    assert JsonEncoder.to_dict(x) == {
        "x1": {"_kind": "marker"},
        "x2": {"y1": {"_kind": "marker"}, "id": {"_kind": "ref", "val": "y1"}},
    }


def test__parse_nested_dict_with_kinds_to_json2():
    x = {
        "x1": kinds.Marker(),
        "x2": {"y1": kinds.Marker(), "id": kinds.Ref("y1")},
        "x3": {
            "y2": kinds.Marker(),
            "id": kinds.Ref("y2"),
            "z1": {"test": kinds.Marker(), "id": kinds.Ref("z1")},
        },
    }

    assert JsonEncoder.to_dict(x) == {
        "x1": {"_kind": "marker"},
        "x2": {"y1": {"_kind": "marker"}, "id": {"_kind": "ref", "val": "y1"}},
        "x3": {
            "y2": {"_kind": "marker"},
            "id": {"_kind": "ref", "val": "y2"},
            "z1": {
                "test": {"_kind": "marker"},
                "id": {"_kind": "ref", "val": "z1"},
            },
        },
    }


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            [
                {"site": {"_kind": "marker"}},
                {"_kind": "dict", "site": {"_kind": "marker"}},
            ],
            [{"site": kinds.Marker()}, {"site": kinds.Marker()}],
        ),
    ],
)
def test_parse_list_of_dicts_to_json(
    test_input: list[dict[str, Any]], expected: list[dict[str, Any]]
) -> None:
    assert JsonDecoder.from_dict(test_input) == expected


def test_grid_to_json_meta1():
    meta = {"test_meta": kinds.Marker()}
    rows = [{"x": 123}, {"y": 456}]
    test_grid = kinds.Grid.to_grid(rows, meta)
    test_json = JsonEncoder.to_dict(test_grid)

    assert test_json["meta"] == {
        "ver": "3.0",
        "test_meta": {"_kind": "marker"},
    }


def test_grid_to_json_meta2():
    meta = {"test_meta": kinds.Marker(), "id": kinds.Ref("test")}
    rows = [{"x": 123}, {"y": 456}]
    test_grid = kinds.Grid.to_grid(rows, meta)
    test_json = JsonEncoder.to_dict(test_grid)

    assert test_json["meta"] == {
        "ver": "3.0",
        "test_meta": {"_kind": "marker"},
        "id": {"_kind": "ref", "val": "test"},
    }


def test_grid_to_json_col1():
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": kinds.Ref("hisA")}},
        {
            "name": "v1",
            "meta": {"id": kinds.Ref("hisB"), "test_col_meta": kinds.Marker()},
        },
        {"name": "v2", "meta": {"id": kinds.Ref("hisC")}},
    ]

    ts1 = datetime.now()
    rows = [
        {
            "ts": ts1 - timedelta(minutes=10),
            "v0": kinds.Number(23, "kW"),
            "v1": kinds.Number(40, "kW"),
            "v2": kinds.Number(50, "kW"),
        },
        {
            "ts": ts1 - timedelta(minutes=5),
            "v0": kinds.Number(23, "kW"),
            "v1": kinds.Number(40, "kW"),
            "v2": kinds.Number(50, "kW"),
        },
        {
            "ts": ts1,
            "v0": kinds.Number(23, "kW"),
            "v1": kinds.Number(40, "kW"),
            "v2": kinds.Number(50, "kW"),
        },
    ]

    test_grid = kinds.Grid(meta, cols, rows)
    test_json = JsonEncoder.to_dict(test_grid)

    assert test_json["meta"] == {"ver": "3.0"}
    assert test_json["cols"][1] == {
        "name": "v0",
        "meta": {"id": {"_kind": "ref", "val": "hisA"}},
    }

    assert test_json["cols"][2] == {
        "name": "v1",
        "meta": {
            "id": {"_kind": "ref", "val": "hisB"},
            "test_col_meta": {"_kind": "marker"},
        },
    }


def test__haystack_to_iana_tz():
    assert _haystack_to_iana_tz("New_York") == ZoneInfo("America/New_York")
    assert _haystack_to_iana_tz("Los_Angeles") == ZoneInfo("America/Los_Angeles")
    assert _haystack_to_iana_tz("Bangkok") == ZoneInfo("Asia/Bangkok")
    assert _haystack_to_iana_tz("UTC") == ZoneInfo("UTC")
    assert _haystack_to_iana_tz("GMT+1") == ZoneInfo("Etc/GMT+1")
    assert _haystack_to_iana_tz("GMT+11") == ZoneInfo("Etc/GMT+11")
    assert _haystack_to_iana_tz("La_Rioja") == ZoneInfo("America/Argentina/La_Rioja")


def test_create_single_his_write_grid():
    meta = {"ver": "3.0", "id": kinds.Ref("hisId")}
    cols = [{"name": "ts"}, {"name": "val"}]
    rows = [
        {
            "ts": datetime.fromisoformat("2012-04-21T08:30:00-04:00").replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
            "val": kinds.Number(72.2),
        },
        {
            "ts": datetime.fromisoformat("2012-04-21T08:45:00-04:00").replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
            "val": kinds.Number(76.3),
        },
    ]

    haystack_grid = kinds.Grid(meta=meta, cols=cols, rows=rows)

    rows_json = [
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:30:00-04:00",
                "tz": "New_York",
            },
            "val": JsonEncoder.to_dict(kinds.Number(72.2)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "val": JsonEncoder.to_dict(kinds.Number(76.3)),
        },
    ]

    assert JsonEncoder.to_dict(haystack_grid)["rows"] == rows_json


def test_create_batch_his_write_grid():
    meta = {"ver": "3.0"}

    cols_haystack = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": kinds.Ref("hisA")}},
        {"name": "v1", "meta": {"id": kinds.Ref("hisB")}},
    ]
    rows_haystack = [
        {
            "ts": datetime.fromisoformat("2012-04-21T08:30:00-04:00").replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
            "v0": kinds.Number(72.2),
            "v1": kinds.Number(10),
        },
        {
            "ts": datetime.fromisoformat("2012-04-21T08:45:00-04:00").replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
            "v0": kinds.Number(76.3),
        },
        {
            "ts": datetime.fromisoformat("2012-04-21T09:00:00-04:00").replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
            "v1": kinds.Number(12),
        },
    ]

    haystack_grid = kinds.Grid(meta=meta, cols=cols_haystack, rows=rows_haystack)

    json_grid = JsonEncoder.to_dict(haystack_grid)

    cols_json = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": {"_kind": "ref", "val": "hisA"}}},
        {"name": "v1", "meta": {"id": {"_kind": "ref", "val": "hisB"}}},
    ]

    assert json_grid["cols"] == cols_json

    rows_json = [
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:30:00-04:00",
                "tz": "New_York",
            },
            "v0": JsonEncoder.to_dict(kinds.Number(72.2)),
            "v1": JsonEncoder.to_dict(kinds.Number(10)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "v0": JsonEncoder.to_dict(kinds.Number(76.3)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T09:00:00-04:00",
                "tz": "New_York",
            },
            "v1": JsonEncoder.to_dict(kinds.Number(12)),
        },
    ]

    assert json_grid["rows"] == rows_json


# -----------------------------------------------------------------------------
# To Grid
# -----------------------------------------------------------------------------


def test__parse_number():
    with pytest.raises(KeyError):
        x = {"_kind": "number", "unit": "kW"}
        JsonDecoder.from_dict(x)

    with pytest.raises(ValueError):
        y = {"_kind": "number", "val": "605.1abc", "unit": "kW"}
        JsonDecoder.from_dict(y)

    # successfully create a Number with unit
    z1 = {"_kind": "number", "val": "605.1", "unit": "kW"}
    assert JsonDecoder.from_dict(z1) == kinds.Number(val=605.1, unit="kW")

    # successfully create a Number without unit
    z2 = {"_kind": "number", "val": "605.1"}
    assert JsonDecoder.from_dict(z2) == kinds.Number(val=605.1, unit=None)


def test__parse_marker():
    assert JsonDecoder.from_dict({"_kind": "marker"}) == kinds.Marker()


def test__parse_remove():
    assert JsonDecoder.from_dict({"_kind": "remove"}) == kinds.Remove()


def test__parse_na():
    assert JsonDecoder.from_dict({"_kind": "na"}) == kinds.NA()


def test__parse_ref():
    with pytest.raises(KeyError):
        x = {"_kind": "ref", "dis": "Elec Meter"}
        JsonDecoder.from_dict(x)

    # successfully create a Ref with dis
    assert kinds.Ref("@foo", "Elec Meter") == JsonDecoder.from_dict(
        {"_kind": "ref", "val": "@foo", "dis": "Elec Meter"}
    )

    # successfully create a Ref without dis
    assert kinds.Ref("@foo") == JsonDecoder.from_dict(
        {"_kind": "ref", "val": "@foo", "dis": None}
    )


def test__parse_date():
    with pytest.raises(KeyError):
        x = {"_kind": "date", "value": "2011-06-07"}
        JsonDecoder.from_dict(x)

    with pytest.raises(ValueError):
        y = {"_kind": "date", "val": "2011-06-07abc"}
        JsonDecoder.from_dict(y)

    assert JsonDecoder.from_dict({"_kind": "date", "val": "2011-06-07"}) == date(
        2011, 6, 7
    )


def test__parse_time():
    with pytest.raises(KeyError):
        x = {"_kind": "time", "value": "14:30:00"}
        JsonDecoder.from_dict(x)

    with pytest.raises(ValueError):
        y = {"_kind": "time", "val": "14:30:00abc"}
        JsonDecoder.from_dict(y)

    assert JsonDecoder.from_dict({"_kind": "time", "val": "14:30:00"}) == time(14, 30)


def test__parse_date_time():
    with pytest.raises(KeyError):
        x = {
            "_kind": "dateTime",
            "val1": "2023-06-20T23:45:00-04:00",
            "tz": "New_York",
        }
        JsonDecoder.from_dict(x)

    with pytest.raises(KeyError):
        y = {
            "_kind": "dateTime",
            "val": "2023-06-20T23:45:00-04:00",
            "tz1": "New_York",
        }
        JsonDecoder.from_dict(y)

    with pytest.raises(ValueError):
        z = {
            "_kind": "dateTime",
            "val": "2023-06-20T23:45:00-04:00abc",
            "tz": "New_York",
        }
        JsonDecoder.from_dict(z)

    a = {
        "_kind": "dateTime",
        "val": "2023-06-20T23:45:00-04:00",
        "tz": "New_York",
    }
    assert JsonDecoder.from_dict(a) == datetime.fromisoformat(a["val"]).replace(
        tzinfo=ZoneInfo("America/New_York")
    )

    b = {
        "_kind": "dateTime",
        "val": "2023-06-20T23:45:00-04:00",
        "tz": "New_Yorkabc",
    }
    with pytest.raises(ValueError):
        JsonDecoder.from_dict(b)


def test__parse_json_dict_value_raises_exception():
    with pytest.raises(ValueError):
        JsonDecoder.from_dict(Decimal(7))


@pytest.mark.parametrize(
    "json_input,expected",
    [
        (
            {
                "_kind": "grid",
                "meta": {"ver": "3.0"},
                "cols": [{"name": "temp"}],
                "rows": [],
            },
            kinds.Grid({"ver": "3.0"}, [kinds.GridCol("temp")], []),
        ),
        (
            {
                "_kind": "grid",
                "meta": {"ver": "3.0"},
                "cols": [
                    {"name": "temp", "meta": {"unit": "°F", "dis": "Temperature"}}
                ],
                "rows": [],
            },
            kinds.Grid(
                {"ver": "3.0"},
                [kinds.GridCol("temp", {"unit": "°F", "dis": "Temperature"})],
                [],
            ),
        ),
        (
            {
                "_kind": "grid",
                "meta": {"ver": "3.0"},
                "cols": [
                    {"name": "id"},
                    {
                        "name": "sensor",
                        "meta": {
                            "id": {"_kind": "ref", "val": "sensor123"},
                            "point": {"_kind": "marker"},
                        },
                    },
                ],
                "rows": [],
            },
            kinds.Grid(
                {"ver": "3.0"},
                [
                    kinds.GridCol("id"),
                    kinds.GridCol(
                        "sensor",
                        {"id": kinds.Ref("sensor123"), "point": kinds.Marker()},
                    ),
                ],
                [],
            ),
        ),
    ],
)
def test_parse_grid_col(json_input: dict[str, Any], expected: kinds.Grid) -> None:
    assert JsonDecoder.from_dict(json_input) == expected


# -----------------------------------------------------------------------------
# To Grid from JSON and back to JSON again with nested data structures
# -----------------------------------------------------------------------------


def test__parse_grid_with_nested_lists_dicts_and_grids():
    json_input = {
        "_kind": "grid",
        "meta": {"ver": "3.0"},
        "cols": [{"name": "type"}, {"name": "val"}],
        "rows": [
            {
                "type": "list",
                "val": [
                    {"_kind": "ref", "val": "foo"},
                    {"_kind": "ref", "val": "bar"},
                ],
            },
            {
                "type": "dict",
                "val": {"dis": "Dict!", "foo": {"_kind": "marker"}},
            },
            {
                "type": "grid",
                "val": {
                    "_kind": "grid",
                    "meta": {"ver": "3.0"},
                    "cols": [{"name": "a"}, {"name": "b"}],
                    "rows": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
                },
            },
            {"type": "scalar", "val": "simple string"},
        ],
    }

    nested_list = [kinds.Ref("foo"), kinds.Ref("bar")]
    nested_dict = {"dis": "Dict!", "foo": kinds.Marker()}
    nested_grid = kinds.Grid.to_grid(
        [
            {"a": kinds.Number(1), "b": kinds.Number(2)},
            {"a": kinds.Number(3), "b": kinds.Number(4)},
        ]
    )

    expected_grid = kinds.Grid.to_grid(
        [
            {"type": "list", "val": nested_list},
            {"type": "dict", "val": nested_dict},
            {"type": "grid", "val": nested_grid},
            {"type": "scalar", "val": "simple string"},
        ]
    )

    grid_from_json = JsonDecoder.from_dict(json_input)
    json_again = JsonEncoder.to_dict(grid_from_json)
    assert json_again == json_input
    assert grid_from_json == expected_grid
