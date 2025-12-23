from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal, getcontext
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from phable.io.ph_decoder import PhDecoder
from phable.kinds import (
    NA,
    Coord,
    Grid,
    GridCol,
    Marker,
    Number,
    PhKind,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
)


class JsonDecoder(PhDecoder):
    def decode(self, data: bytes) -> PhKind:
        return _parse_val(json.loads(data.decode("utf-8")))

    def from_str(self, data: str) -> PhKind:
        return _parse_val(json.loads(data))

    @staticmethod
    def from_json(data: dict[str, Any] | list[dict[str, Any]]) -> PhKind:
        return _parse_val(data)


def _parse_val(value: Any) -> PhKind:
    match value:
        case bool() | str():
            return value
        case int() | float():
            return Number(value, None)
        case dict():
            if "_kind" in value.keys():
                return _to_kind(value)
            else:
                return _parse_dict(value)
        case list():
            return _parse_list(value)
        case _:
            raise ValueError()


def _parse_grid(json_data: dict[str, Any]) -> Grid:
    dict_data = _parse_dict(json_data)

    cols = [_parse_grid_col(col) for col in dict_data["cols"]]

    return Grid(
        meta=dict_data["meta"],
        cols=cols,
        rows=dict_data["rows"],
    )


def _parse_dict(value_dict: dict[str, Any]) -> dict[str, Any]:
    parsed_dict = {}
    for key, value in value_dict.items():
        if key == "_kind" and value == "dict":
            continue
        parsed_dict[key] = _parse_val(value)

    return parsed_dict


def _parse_list(value_list: list[Any]) -> list[Any]:
    return [_parse_val(x) for x in value_list]


def _to_kind(d: dict[str, str]):
    parse_map = {
        "number": _parse_number,
        "marker": _parse_marker,
        "remove": _parse_remove,
        "na": _parse_na,
        "ref": _parse_ref,
        "dict": _parse_dict,
        "date": _parse_date,
        "time": _parse_time,
        "dateTime": _parse_date_time,
        "uri": _parse_uri,
        "coord": _parse_coord,
        "xstr": _parse_xstr,
        "symbol": _parse_symbol,
        "grid": _parse_grid,
    }

    return parse_map[d["_kind"]](d)


def _parse_number(d: dict[str, str]) -> Number:
    unit = d.get("unit", None)
    num = float(d["val"])

    return Number(num, unit)


def _parse_marker(d: dict[str, str]) -> Marker:
    return Marker()


def _parse_remove(d: dict[str, str]) -> Remove:
    return Remove()


def _parse_na(d: dict[str, str]) -> NA:
    return NA()


def _parse_ref(d: dict[str, str]) -> Ref:
    dis = d.get("dis", None)
    return Ref(d["val"], dis)


def _parse_date(d: dict[str, str]) -> date:
    return date.fromisoformat(d["val"])


def _parse_time(d: dict[str, str]) -> time:
    return time.fromisoformat(d["val"])


@lru_cache(maxsize=16)
def _haystack_to_iana_tz(haystack_tz: str) -> ZoneInfo:
    for iana_tz in available_timezones():
        if "UTC" in haystack_tz:
            return ZoneInfo("UTC")
        elif haystack_tz == _tz_iana_to_haystack(iana_tz):
            return ZoneInfo(iana_tz)

    raise ValueError(
        f"Unable to locate the city {haystack_tz} in the IANA database.  "
        + "Consider installing the tzdata package."
    )


def _tz_iana_to_haystack(iana_tz: str) -> str:
    if "UTC" in iana_tz:
        return "UTC"
    return iana_tz.split("/")[-1]


def _parse_date_time(d: dict[str, str]) -> datetime:
    haystack_tz: str = d["tz"]
    iana_tz: ZoneInfo = _haystack_to_iana_tz(haystack_tz)
    dt = datetime.fromisoformat(d["val"]).astimezone(iana_tz)
    return dt


def _parse_uri(d: dict[str, str]) -> Uri:
    return Uri(d["val"])


def _parse_coord(d: dict[str, str]) -> Coord:
    getcontext().prec = 6
    lat = Decimal(d["lat"])
    lng = Decimal(d["lng"])
    return Coord(lat, lng)


def _parse_xstr(d: dict[str, str]) -> XStr:
    return XStr(d["type"], d["val"])


def _parse_symbol(d: dict[str, str]) -> Symbol:
    return Symbol(d["val"])


def _parse_grid_col(d: dict[str, Any]) -> GridCol:
    name = d["name"]
    meta = d.get("meta")
    return GridCol(name, meta)
