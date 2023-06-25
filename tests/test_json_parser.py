import logging
from datetime import date, datetime, time

import pytest

import phable.kinds as kinds
from phable.parser.json import (
    NotFoundError,
    _haystack_to_iana_tz,
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
)

logger = logging.getLogger(__name__)


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
    with pytest.raises(NotFoundError):
        _parse_date_time(b)
