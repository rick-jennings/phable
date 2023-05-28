from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

import pandas as pd

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
    parse_kinds(d)
    return Grid(meta=d["meta"], cols=d["cols"], rows=d["rows"])


def parse_kinds(d: dict[str, Any]):
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
                new_d[x] = to_kind(new_d[x])


# TODO :  Map Haystack kinds in DF to Pandas types
def grid_to_pandas(g: Grid) -> pd.DataFrame:
    # initialize the pandas df
    col_names = [col_grid["name"] for col_grid in g.cols]
    df = pd.DataFrame(data=g.rows, columns=col_names)

    # make column names involving timeseries data more human readable
    name_map = {}
    for col in g.cols:
        if col["name"] == "ts":
            name_map["ts"] = "Timestamp"

        if "meta" in col.keys():
            if "id" in col["meta"].keys():
                name_map[col["name"]] = col["meta"]["id"].val

    # rename the columns in the df to be easier to read
    df = df.rename(columns=name_map)

    return df


def to_kind(d: dict[str, str]):
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


@dataclass
class NotFoundError(Exception):
    help_msg: str


def _parse_number(d: dict[str, str]) -> Number:
    unit = d.get("unit", None)

    try:
        return Number(float(d["val"]), unit)
    except KeyError:
        logger.debug(
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(f"Unable to parse the 'val' key's value into a float:\n{d}")
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
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise


def _parse_date(d: dict[str, str]):
    try:
        return Date(date.fromisoformat(d["val"]))
    except KeyError:
        logger.debug(
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(f"Unable to parse the 'val' key's value into a date:\n{d}")
        raise


def _parse_time(d: dict[str, str]):
    try:
        return Time(time.fromisoformat(d["val"]))
    except KeyError:
        logger.debug(
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(f"Unable to parse the 'val' key's value into a time:\n{d}")
        raise


def _build_iana_tz(haystack_tz: str) -> str:
    for iana_tz in available_timezones():
        if "/" + haystack_tz in iana_tz:
            return iana_tz

    raise NotFoundError(f"Can't locate the city {haystack_tz} in the IANA database")


def haystack_to_iana_tz(haystack_tz: str) -> ZoneInfo:
    if haystack_tz in available_timezones():
        iana_tz = haystack_tz
    else:
        iana_tz = _build_iana_tz(haystack_tz)

    return ZoneInfo(iana_tz)


def _parse_date_time(d: dict[str, str]):
    try:
        haystack_tz: str = d["tz"]
        iana_tz: ZoneInfo = haystack_to_iana_tz(haystack_tz)
        dt = datetime.fromisoformat(d["val"]).astimezone(iana_tz)
        return DateTime(dt, haystack_tz)
    except KeyError:
        logger.debug(
            "Received this input which did not have the expected 'val' or 'tz' key:"
            + f"\n{d}"
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
