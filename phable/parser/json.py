from __future__ import annotations

import logging
from datetime import date, datetime, time
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from phable.exceptions import NotFoundError
from phable.kinds import (
    NA,
    Coordinate,
    Date,
    DateTime,
    Grid,
    Marker,
    Number,
    Ref,
    Remove,
    Symbol,
    Time,
    Uri,
    XStr,
)

logger = logging.getLogger(__name__)


def json_to_grid(d: dict[str, Any]) -> Grid:
    _parse_kinds(d)
    return Grid(meta=d["meta"], cols=d["cols"], rows=d["rows"])


def create_his_write_grid(
    ids: Ref | list[Ref], data: list[dict[str, Any]]
) -> Grid:
    """
    Note:  The order of the ids are important!
    """
    meta: dict[str, str | dict[str, str]] = {"ver": "3.0"}
    cols: list[dict[str, Any]] = [{"name": "ts"}]
    if isinstance(ids, Ref):
        meta["id"] = _ref_to_json(ids)
        cols.append({"name": "val"})
    else:
        for count, id in enumerate(ids):
            cols.append(
                {"name": "v" + str(count), "meta": {"id": _ref_to_json(id)}}
            )

    # TODO: traverse the rows and parse to JSON!
    rows: list[dict[str, str | dict[str, str]]] = []
    for row in data:
        if len(row) > len(cols):
            # TODO:  improve this exception
            raise Exception
        rows.append(_parse_row_to_json(row))

    return Grid(meta, cols, rows)


def _parse_row_to_json(row: dict[str, Any]) -> dict[str, str | dict[str, str]]:
    parsed_row: dict[str, str | dict[str, str]] = {}
    for key in row.keys():
        val = row[key]
        if isinstance(val, DateTime):
            parsed_row[key] = _datetime_to_json(val)
        elif isinstance(row[key], Number):
            parsed_row[key] = _number_to_json(val)  # type: ignore
        elif isinstance(row[key], str):
            parsed_row[key] = val
        elif isinstance(row[key], bool):
            parsed_row[key] = val
        else:
            # TODO: create a unique exception for this case
            raise Exception
    return parsed_row


def _number_to_json(num: Number) -> dict[str, str | float]:
    json = {"_kind": "number", "val": num.val}
    if num.unit is not None:
        json["unit"] = num.unit
    return json


def _datetime_to_json(date_time: DateTime) -> dict[str, str]:
    json = {"_kind": "dateTime", "val": date_time.val.isoformat()}
    if date_time.tz is not None:
        json["tz"] = date_time.tz
    return json


def _ref_to_json(ref: Ref) -> dict[str, str]:
    json = {"_kind": "ref", "val": ref.val}
    if ref.dis is not None:
        json["dis"] = ref.dis
    return json


def _parse_kinds(d: dict[str, Any]):
    """Traverse JSON and convert where needed to Haystack kinds.

    Args:
        d (dict[str, Any]): _description_

    Returns:
        _type_: _description_
    """

    # input d is a mutable object, so we want to modify a copy of it
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
        if isinstance(new_d[x], dict):
            if "_kind" in new_d[x].keys():
                new_d[x] = _to_kind(new_d[x])


def _to_kind(d: dict[str, str]):
    # test to make sure d is a Dict

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

    try:
        return Number(float(d["val"]), unit)
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' "
            f"key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(
            f"Unable to parse the 'val' key's value into a float:\n{d}"
        )
        raise


def _parse_marker(d: dict[str, str]) -> Marker:
    return Marker()


def _parse_remove(d: dict[str, str]) -> Remove:
    return Remove()


def _parse_na(d: dict[str, str]) -> NA:
    return NA()


def _parse_ref(d: dict[str, str]) -> Ref:
    try:
        dis = d.get("dis", None)
        return Ref(d["val"], dis)
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' key:\n"
            f"{d}"
        )
        raise


def _parse_date(d: dict[str, str]):
    try:
        return Date(date.fromisoformat(d["val"]))
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' key:\n"
            f"{d}"
        )
        raise
    except ValueError:
        logger.debug(
            f"Unable to parse the 'val' key's value into a date:\n{d}"
        )
        raise


def _parse_time(d: dict[str, str]):
    try:
        return Time(time.fromisoformat(d["val"]))
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' key:\n"
            f"{d}"
        )
        raise
    except ValueError:
        logger.debug(
            f"Unable to parse the 'val' key's value into a time:\n{d}"
        )
        raise


@lru_cache(maxsize=16)
def _haystack_to_iana_tz(haystack_tz: str) -> ZoneInfo:
    for iana_tz in available_timezones():
        if "/" + haystack_tz in iana_tz or haystack_tz == iana_tz:
            return ZoneInfo(iana_tz)

    # future: maybe return None instead of raising error?
    raise NotFoundError(
        f"Can't locate the city {haystack_tz} in the IANA database"
    )


def _parse_date_time(d: dict[str, str]):
    try:
        haystack_tz: str = d["tz"]
        iana_tz: ZoneInfo = _haystack_to_iana_tz(haystack_tz)
        dt = datetime.fromisoformat(d["val"]).astimezone(iana_tz)
        return DateTime(dt, haystack_tz)
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' or 'tz'"
            f"key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(f"Unable to parse the 'val' or 'tz' key value:\n{d}")
        raise


def _parse_uri(d: dict[str, str]):
    return Uri(d["val"])


def _parse_coord(d: dict[str, str]):
    lat = float(d["lat"])
    lng = float(d["lng"])
    return Coordinate(lat, lng)


def _parse_xstr(d: dict[str, str]):
    return XStr(d["type"], d["val"])


def _parse_symbol(d: dict[str, str]):
    return Symbol(d["val"])
