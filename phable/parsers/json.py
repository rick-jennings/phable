from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, getcontext
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from phable.kinds import NA, Coord, Grid, Marker, Number, Ref, Remove, Symbol, Uri, XStr

# -----------------------------------------------------------------------------
# To JSON
# -----------------------------------------------------------------------------


@dataclass
class HaystackKindToJsonParsingError(Exception):
    help_msg: str


def grid_to_json(grid: Grid) -> dict[str, Any]:
    return {
        "_kind": "grid",
        "meta": _kind_to_json(grid.meta),
        "cols": _kind_to_json(grid.cols),
        "rows": _kind_to_json(grid.rows),
    }


def _kind_to_json(kind: Any) -> dict[str, Any]:
    match kind:
        case int() | float() | str() | bool():
            return kind
        case datetime():
            return _datetime_to_json(kind)
        case date():
            return {"_kind": "date", "val": kind.isoformat()}
        case time():
            return {"_kind": "time", "val": kind.isoformat()}
        case Number():
            return _number_to_json(kind)
        case Ref():
            return _ref_to_json(kind)
        case Symbol():
            return {"_kind": "symbol", "val": kind.val}
        case Marker():
            return {"_kind": "marker"}
        case NA():
            return {"_kind": "na"}
        case Remove():
            return {"_kind": "remove"}
        case Uri():
            return {"_kind": "uri", "val": kind.val}
        case Coord():
            return {
                "_kind": "coord",
                "lat": float(kind.lat),
                "lng": float(kind.lng),
            }
        case XStr():
            return {"_kind": "xstr", "type": kind.type, "val": kind.val}
        case dict():
            return _dict_to_json(kind)
        case list():
            return [_kind_to_json(x) for x in kind]
        case Grid():
            return grid_to_json(kind)
        case _:
            raise HaystackKindToJsonParsingError(
                f"Unable to parse input {kind} with Python type {type(kind)} into JSON"
            )


def _dict_to_json(row: dict[str, Any]) -> dict[str, Any]:
    parsed_row: dict[str, str | dict[str, str]] = {}
    for key in row.keys():
        parsed_row[key] = _kind_to_json(row[key])

    return parsed_row


def _number_to_json(num: Number) -> int | float | dict[str, str | float]:
    if num.unit is None:
        return num.val
    return {"_kind": "number", "val": num.val, "unit": num.unit}


def _datetime_to_json(date_time: datetime) -> dict[str, str]:
    iana_tz = str(date_time.tzinfo)
    if "/" in iana_tz:
        haystack_tz = iana_tz.split("/")[-1]
    else:
        haystack_tz = iana_tz

    json = {
        "_kind": "dateTime",
        "val": date_time.isoformat(),
        "tz": haystack_tz,
    }

    return json


def _ref_to_json(ref: Ref) -> dict[str, str]:
    json = {"_kind": "ref", "val": ref.val}
    if ref.dis is not None:
        json["dis"] = ref.dis
    return json


# -----------------------------------------------------------------------------
# To Grid
# -----------------------------------------------------------------------------


def json_to_grid(json_data: dict[str, Any]) -> Grid:
    dict_data = _parse_dict(json_data)

    return Grid(meta=dict_data["meta"], cols=dict_data["cols"], rows=dict_data["rows"])


def _parse_dict(value_dict: dict[str, Any]) -> dict[str, Any]:
    return {key: _parse_value(value) for key, value in value_dict.items()}


def _parse_list(value_list: list[Any]) -> list[Any]:
    return [_parse_value(x) for x in value_list]


def _parse_value(value: Any) -> Any:
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
            raise HaystackKindToJsonParsingError(f"Unable to parse dict val:  {value}")


def _to_kind(d: dict[str, str]):
    parse_map = {
        "number": _parse_number,
        "marker": _parse_marker,
        "remove": _parse_remove,
        "na": _parse_na,
        "ref": _parse_ref,
        "date": _parse_date,
        "time": _parse_time,
        "dateTime": _parse_date_time,
        "uri": _parse_uri,
        "coord": _parse_coord,
        "xstr": _parse_xstr,
        "symbol": _parse_symbol,
        "grid": json_to_grid,
    }

    return parse_map[d["_kind"]](d)


def _parse_number(d: dict[str, str]) -> Number:
    unit = d.get("unit", None)
    num = float(d["val"])

    if num % 1 == 0:
        num = int(num)

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


@dataclass
class IanaCityNotFoundError(Exception):
    help_msg: str


@lru_cache(maxsize=16)
def _haystack_to_iana_tz(haystack_tz: str) -> ZoneInfo:
    for iana_tz in available_timezones():
        if "UTC" in haystack_tz:
            return ZoneInfo("UTC")
        elif haystack_tz == iana_tz.split("/")[-1]:
            return ZoneInfo(iana_tz)

    raise IanaCityNotFoundError(
        f"Unable to locate the city {haystack_tz} in the IANA database.  "
        + "Consider installing the tzdata package."
    )


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
    return Coord(lat, lng)  # type: ignore


def _parse_xstr(d: dict[str, str]) -> XStr:
    return XStr(d["type"], d["val"])


def _parse_symbol(d: dict[str, str]) -> Symbol:
    return Symbol(d["val"])
