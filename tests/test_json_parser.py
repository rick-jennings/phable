from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

import phable.kinds as kinds
from phable.parsers.json import (
    HaystackKindToJsonParsingError,
    IanaCityNotFoundError,
    _dict_to_json,
    _haystack_to_iana_tz,
    _kind_to_json,
    _parse_date,
    _parse_date_time,
    _parse_marker,
    _parse_na,
    _parse_number,
    _parse_ref,
    _parse_remove,
    _parse_time,
    grid_to_json,
)

# -----------------------------------------------------------------------------
# To JSON - tests for Kind to JSON
# -----------------------------------------------------------------------------


def test_datetime_to_json():
    now = datetime.now(ZoneInfo("America/New_York"))
    assert _kind_to_json(now) == {
        "_kind": "dateTime",
        "val": now.isoformat(),
        "tz": "New_York",
    }


def test_date_to_json():
    today = date(2024, 3, 27)
    assert _kind_to_json(today) == {"_kind": "date", "val": "2024-03-27"}


def test_time_to_json():
    t1 = time(12, 12, 59)
    assert _kind_to_json(t1) == {"_kind": "time", "val": "12:12:59"}


def test_number_to_json():
    x = kinds.Number(20, "kW")
    assert _kind_to_json(x) == {"_kind": "number", "val": 20, "unit": "kW"}

    y = kinds.Number(20)
    assert _kind_to_json(y) == {"_kind": "number", "val": 20}


def test_int_to_json():
    x = 24
    assert _kind_to_json(x) == 24


def test_float_to_json():
    x = 24.1
    assert _kind_to_json(x) == 24.1


def test_str_to_json():
    x = "Hello World!"
    assert _kind_to_json(x) == x


def test_bool_to_json():
    x = True
    assert _kind_to_json(x) == x


def test_ref_to_json():
    ref_id = "abc1234"
    assert _kind_to_json(kinds.Ref(ref_id)) == {"_kind": "ref", "val": ref_id}

    assert _kind_to_json(kinds.Ref(ref_id, "Carytown")) == {
        "_kind": "ref",
        "val": ref_id,
        "dis": "Carytown",
    }


def test_symbol_to_json():
    x = kinds.Symbol("abc")
    assert _kind_to_json(x) == {"_kind": "symbol", "val": "abc"}


def test_marker_to_json():
    x = kinds.Marker()
    assert _kind_to_json(x) == {"_kind": "marker"}


def test_na_to_json():
    x = kinds.NA()
    assert _kind_to_json(x) == {"_kind": "na"}


def test_remove_to_json():
    x = kinds.Remove()
    assert _kind_to_json(x) == {"_kind": "remove"}


def test_uri_to_json():
    x = kinds.Uri("https://project-haystack.org")
    assert _kind_to_json(x) == {
        "_kind": "uri",
        "val": "https://project-haystack.org",
    }


def test_coord_to_json():
    x = kinds.Coord(Decimal("37.548266"), Decimal("-77.4491888"))
    assert _kind_to_json(x) == {
        "_kind": "coord",
        "lat": 37.548266,
        "lng": -77.4491888,
    }


def test_xstr_to_json():
    x = kinds.XStr("value", "red")
    assert _kind_to_json(x) == {"_kind": "xstr", "type": "value", "val": "red"}


def test_list_to_json():
    x = [
        kinds.Number(12, "kW"),
        True,
        {"test": kinds.Marker()},
        kinds.Grid.to_grid({"id": kinds.Ref("test1"), "dis": "test1"}),
    ]
    assert _kind_to_json(x) == [
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


def test_grid_to_json():
    nested_grid = kinds.Grid.to_grid([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    rows = [
        {"type": "list", "val": [1, 2, 3]},
        {"type": "dict", "val": {"dis": "Dict!", "foo": kinds.Marker()}},
        {"type": "grid", "val": nested_grid},
        {"type": "scalar", "val": "simple string"},
    ]

    x = kinds.Grid.to_grid(rows)

    expected_json = {
        "_kind": "grid",
        "meta": {"ver": "3.0"},
        "cols": [{"name": "type"}, {"name": "val"}],
        "rows": [
            {"type": "list", "val": [1, 2, 3]},
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

    assert _kind_to_json(x) == expected_json


def test_kind_to_json_raises_error():
    with pytest.raises(HaystackKindToJsonParsingError):
        _kind_to_json(timedelta(days=5))


# -----------------------------------------------------------------------------
# To JSON - Dictionaries and Grids
# -----------------------------------------------------------------------------


def test__parse_dict_with_kinds_to_json():

    x = {"test_meta": kinds.Marker()}

    assert _dict_to_json(x) == {"test_meta": {"_kind": "marker"}}


def test__parse_nested_dict_with_kinds_to_json1():

    x = {
        "x1": kinds.Marker(),
        "x2": {"y1": kinds.Marker(), "id": kinds.Ref("y1")},
    }

    assert _dict_to_json(x) == {
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

    assert _dict_to_json(x) == {
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


def test_grid_to_json_meta1():
    meta = {"test_meta": kinds.Marker()}
    rows = [{"x": 123}, {"y": 456}]
    test_grid = kinds.Grid.to_grid(rows, meta)
    test_json = grid_to_json(test_grid)

    assert test_json["meta"] == {
        "ver": "3.0",
        "test_meta": {"_kind": "marker"},
    }


def test_grid_to_json_meta2():
    meta = {"test_meta": kinds.Marker(), "id": kinds.Ref("test")}
    rows = [{"x": 123}, {"y": 456}]
    test_grid = kinds.Grid.to_grid(rows, meta)
    test_json = grid_to_json(test_grid)

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
    test_json = grid_to_json(test_grid)

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
    assert _haystack_to_iana_tz("Los_Angeles") == ZoneInfo(
        "America/Los_Angeles"
    )
    assert _haystack_to_iana_tz("Bangkok") == ZoneInfo("Asia/Bangkok")
    assert _haystack_to_iana_tz("UTC") == ZoneInfo("UTC")
    assert _haystack_to_iana_tz("GMT+1") == ZoneInfo("Etc/GMT+1")
    assert _haystack_to_iana_tz("GMT+11") == ZoneInfo("Etc/GMT+11")
    assert _haystack_to_iana_tz("La_Rioja") == ZoneInfo(
        "America/Argentina/La_Rioja"
    )


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
            "val": _kind_to_json(kinds.Number(72.2)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "val": _kind_to_json(kinds.Number(76.3)),
        },
    ]

    assert grid_to_json(haystack_grid)["rows"] == rows_json


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

    haystack_grid = kinds.Grid(
        meta=meta, cols=cols_haystack, rows=rows_haystack
    )

    json_grid = grid_to_json(haystack_grid)

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
            "v0": _kind_to_json(kinds.Number(72.2)),
            "v1": _kind_to_json(kinds.Number(10)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "v0": _kind_to_json(kinds.Number(76.3)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T09:00:00-04:00",
                "tz": "New_York",
            },
            "v1": _kind_to_json(kinds.Number(12)),
        },
    ]

    assert json_grid["rows"] == rows_json


# -----------------------------------------------------------------------------
# To Grid
# -----------------------------------------------------------------------------


def test__parse_number():
    with pytest.raises(KeyError):
        x = {"unit": "kW"}
        _parse_number(x)

    with pytest.raises(ValueError):
        y = {"val": "605.1abc", "unit": "kW"}
        _parse_number(y)

    # successfully create a Number with unit
    z1 = {"val": "605.1", "unit": "kW"}
    assert _parse_number(z1) == kinds.Number(val=605.1, unit="kW")

    # successfully create a Number without unit
    z2 = {"_kind": "number", "val": "605.1"}
    assert _parse_number(z2) == kinds.Number(val=605.1, unit=None)


def test__parse_marker():
    assert _parse_marker({}) == kinds.Marker()


def test__parse_remove():
    assert _parse_remove({}) == kinds.Remove()


def test__parse_na():
    assert _parse_na({}) == kinds.NA()


def test__parse_ref():
    with pytest.raises(KeyError):
        x = {"dis": "Elec Meter"}
        _parse_ref(x)

    # successfully create a Ref with dis
    assert kinds.Ref("@foo", "Elec Meter") == _parse_ref(
        {"val": "@foo", "dis": "Elec Meter"}
    )

    # successfully create a Ref without dis
    assert kinds.Ref("@foo") == _parse_ref({"val": "@foo", "dis": None})


def test__parse_date():
    with pytest.raises(KeyError):
        x = {"value": "2011-06-07"}
        _parse_date(x)

    with pytest.raises(ValueError):
        y = {"val": "2011-06-07abc"}
        _parse_date(y)

    assert _parse_date({"val": "2011-06-07"}) == date(2011, 6, 7)


def test__parse_time():
    with pytest.raises(KeyError):
        x = {"value": "14:30:00"}
        _parse_time(x)

    with pytest.raises(ValueError):
        y = {"val": "14:30:00abc"}
        _parse_time(y)

    assert _parse_time({"val": "14:30:00"}) == time(14, 30)


def test__parse_date_time():
    with pytest.raises(KeyError):
        x = {
            "_kind": "dateTime",
            "val1": "2023-06-20T23:45:00-04:00",
            "tz": "New_York",
        }
        _parse_date_time(x)

    with pytest.raises(KeyError):
        y = {
            "_kind": "dateTime",
            "val": "2023-06-20T23:45:00-04:00",
            "tz1": "New_York",
        }
        _parse_date_time(y)

    with pytest.raises(ValueError):
        z = {
            "_kind": "dateTime",
            "val": "2023-06-20T23:45:00-04:00abc",
            "tz": "New_York",
        }
        _parse_date_time(z)

    a = {
        "_kind": "dateTime",
        "val": "2023-06-20T23:45:00-04:00",
        "tz": "New_York",
    }
    assert _parse_date_time(a) == datetime.fromisoformat(a["val"]).replace(
        tzinfo=ZoneInfo("America/New_York")
    )

    b = {
        "_kind": "dateTime",
        "val": "2023-06-20T23:45:00-04:00",
        "tz": "New_Yorkabc",
    }
    with pytest.raises(IanaCityNotFoundError):
        _parse_date_time(b)
