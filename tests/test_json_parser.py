from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

import phable.kinds as kinds
from phable.parser.json import (
    _datetime_to_json,
    _haystack_to_iana_tz,
    _number_to_json,
    _parse_coord,
    _parse_date,
    _parse_date_time,
    _parse_marker,
    _parse_na,
    _parse_number,
    _parse_ref,
    _parse_remove,
    _parse_symbol,
    _parse_time,
    _parse_uri,
    _parse_xstr,
    _ref_to_json,
    create_his_write_grid,
    IanaCityNotFoundError
)


def test_create_single_his_write_grid():
    meta = {"ver": "3.0", "id": {"_kind": "ref", "val": "hisId"}}
    cols = [{"name": "ts"}, {"name": "val"}]
    rows_haystack = [
        {
            "ts": kinds.DateTime(
                datetime.fromisoformat("2012-04-21T08:30:00-04:00"), "New_York"
            ),
            "val": kinds.Number(72.2),
        },
        {
            "ts": kinds.DateTime(
                datetime.fromisoformat("2012-04-21T08:45:00-04:00"), "New_York"
            ),
            "val": kinds.Number(76.3),
        },
    ]
    rows_json = [
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:30:00-04:00",
                "tz": "New_York",
            },
            "val": _number_to_json(kinds.Number(72.2)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "val": _number_to_json(kinds.Number(76.3)),
        },
    ]

    assert create_his_write_grid(
        kinds.Ref("hisId"), rows_haystack
    ) == kinds.Grid(meta, cols, rows_json)


def test_create_batch_his_write_grid():
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": {"_kind": "ref", "val": "hisA"}}},
        {"name": "v1", "meta": {"id": {"_kind": "ref", "val": "hisB"}}},
    ]
    rows_haystack = [
        {
            "ts": kinds.DateTime(
                datetime.fromisoformat("2012-04-21T08:30:00-04:00"),
                "New_York",
            ),
            "v0": kinds.Number(72.2),
            "v1": kinds.Number(10),
        },
        {
            "ts": kinds.DateTime(
                datetime.fromisoformat("2012-04-21T08:45:00-04:00"),
                "New_York",
            ),
            "v0": kinds.Number(76.3),
        },
        {
            "ts": kinds.DateTime(
                datetime.fromisoformat("2012-04-21T09:00:00-04:00"),
                "New_York",
            ),
            "v1": kinds.Number(12),
        },
    ]
    rows_json = [
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:30:00-04:00",
                "tz": "New_York",
            },
            "v0": _number_to_json(kinds.Number(72.2)),
            "v1": _number_to_json(kinds.Number(10)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T08:45:00-04:00",
                "tz": "New_York",
            },
            "v0": _number_to_json(kinds.Number(76.3)),
        },
        {
            "ts": {
                "_kind": "dateTime",
                "val": "2012-04-21T09:00:00-04:00",
                "tz": "New_York",
            },
            "v1": _number_to_json(kinds.Number(12)),
        },
    ]

    test_grid = create_his_write_grid(
        [kinds.Ref("hisA"), kinds.Ref("hisB")], rows_haystack
    )

    assert test_grid == kinds.Grid(meta, cols, rows_json)


def test__number_to_json():
    x = kinds.Number(20, "kW")
    assert _number_to_json(x) == {"_kind": "number", "val": 20, "unit": "kW"}

    y = kinds.Number(20)
    assert _number_to_json(y) == {"_kind": "number", "val": 20}


def test__datetime_to_json():
    now = datetime.now(ZoneInfo("America/New_York"))
    assert _datetime_to_json(kinds.DateTime(now)) == {
        "_kind": "dateTime",
        "val": now.isoformat(),
    }

    assert _datetime_to_json(kinds.DateTime(now, "New_York")) == {
        "_kind": "dateTime",
        "val": now.isoformat(),
        "tz": "New_York",
    }


def test__ref_to_json():
    ref_id = "abc1234"
    assert _ref_to_json(kinds.Ref(ref_id)) == {"_kind": "ref", "val": ref_id}

    assert _ref_to_json(kinds.Ref(ref_id, "Carytown")) == {
        "_kind": "ref",
        "val": ref_id,
        "dis": "Carytown",
    }


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

    assert _parse_date({"val": "2011-06-07"}) == kinds.Date(date(2011, 6, 7))


def test__parse_time():
    with pytest.raises(KeyError):
        x = {"value": "14:30:00"}
        _parse_time(x)

    with pytest.raises(ValueError):
        y = {"val": "14:30:00abc"}
        _parse_time(y)

    assert _parse_time({"val": "14:30:00"}) == kinds.Time(time(14, 30))


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
    assert _parse_date_time(a) == kinds.DateTime(
        datetime.fromisoformat(a["val"]), "New_York"
    )

    b = {
        "_kind": "dateTime",
        "val": "2023-06-20T23:45:00-04:00",
        "tz": "New_Yorkabc",
    }
    with pytest.raises(IanaCityNotFoundError):
        _parse_date_time(b)
