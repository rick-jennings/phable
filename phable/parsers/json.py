from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, getcontext
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from phable.kinds import NA, Coord, Grid, Marker, Number, Ref, Remove, Symbol, Uri, XStr


def json_to_grid(json_data: dict[str, Any]) -> Grid:
    _parse_kinds(json_data)

    return Grid(meta=json_data["meta"], cols=json_data["cols"], rows=json_data["rows"])


# TODO: need to make this more robust and add tests, etc.
def grid_to_json(grid: Grid) -> dict[str, Any]:
    meta = grid.meta
    cols = grid.cols
    rows = grid.rows

    # traverse the meta and parse to JSON
    new_meta: dict[str, Any] = meta.copy()
    for m in meta.keys():
        if m == "id":
            new_meta["id"] = _ref_to_json(new_meta["id"])

    # traverse the cols and parse to JSON
    new_cols: list[dict[str, Any]] = []
    for col in cols:
        new_cols.append(_parse_dict_with_kinds_to_json(col))

    # traverse the rows and parse to JSON
    new_rows: list[dict[str, str | dict[str, str]]] = []
    for row in rows:
        new_rows.append(_parse_dict_with_kinds_to_json(row))

    return {
        "_kind": "grid",
        "meta": new_meta,
        "cols": new_cols,
        "rows": new_rows,
    }


# TODO
def parse_kind_to_json(kind: Any) -> dict[str, str | dict[str, str]]: ...


def _parse_dict_with_kinds_to_json(
    row: dict[str, Any]
) -> dict[str, str | dict[str, str]]:
    parsed_row: dict[str, str | dict[str, str]] = {}
    for key in row.keys():
        val = row[key]
        if isinstance(val, datetime):
            parsed_row[key] = _datetime_to_json(val)
        elif isinstance(row[key], Number):
            parsed_row[key] = _number_to_json(val)
        elif isinstance(row[key], str):
            parsed_row[key] = val
        elif isinstance(row[key], bool):
            parsed_row[key] = val
        elif isinstance(row[key], Ref):
            parsed_row[key] = _ref_to_json(val)
        # this case is for dealing with col data in Grid
        elif isinstance(row[key], Marker):
            parsed_row[key] = {"_kind": "marker"}
        elif isinstance(row[key], NA):
            parsed_row[key] = {"_kind": "na"}
        elif isinstance(row[key], Remove):
            parsed_row[key] = {"_kind": "remove"}
        elif isinstance(row[key], dict):
            if "id" in row[key].keys():
                new_val = row[key]
                new_val["id"] = _ref_to_json(row[key]["id"])
                parsed_row[key] = new_val
            # TODO: add some more checks here
        else:
            # TODO: create a unique exception for this case
            raise Exception
    return parsed_row


def _number_to_json(num: Number) -> dict[str, str | float]:
    json = {"_kind": "number", "val": num.val}
    if num.unit is not None:
        json["unit"] = num.unit
    return json


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


def _parse_kinds(d: dict[str, Any]):
    """Traverse JSON and convert where needed to Haystack kinds."""
    new_d = d.copy()

    # parse grid meta
    _parse_layer(new_d["meta"])

    # parse col meta
    for i in range(len(new_d["cols"])):
        if "meta" in new_d["cols"][i].keys():
            _parse_layer(new_d["cols"][i]["meta"])

    # parse rows
    for i in range(len(new_d["rows"])):
        _parse_layer(new_d["rows"][i])

    return new_d


def _parse_layer(new_d: dict[str, Any]) -> None:
    for x in new_d.keys():
        if isinstance(new_d[x], int):
            new_d[x] = Number(new_d[x], None)
        elif isinstance(new_d[x], float):
            new_d[x] = Number(new_d[x], None)
        elif isinstance(new_d[x], dict):
            if "_kind" in new_d[x].keys():
                new_d[x] = _to_kind(new_d[x])


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
        f"Unable to locate the city {haystack_tz} in the IANA database"
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
