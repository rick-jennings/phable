from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

import pandas as pd

import phable.kinds as kinds
from phable.kinds import (
    NA,
    Coordinate,
    Date,
    DateTime,
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


def json_to_grid(d: dict[str, Any]) -> kinds.Grid:
    parse_kinds(d)
    return kinds.Grid(meta=d["meta"], cols=d["cols"], rows=d["rows"])


# # NOTE :  Under development !  Do not use grid_to_json for now
# # TODO :  We need to traverse the dicts like we did when going from json to grid
# def grid_to_json(g: kinds.Grid) -> dict[Any, Any]:
#     meta = _kind_in_dict_to_str(g.meta)
#     cols = [_kind_in_dict_to_str(col) for col in g.cols]
#     rows = [_kind_in_dict_to_str(row) for row in g.rows]

#     return {"_kind": "grid", "meta": meta, "cols": cols, "rows": rows}


# # TODO :  Confirm its not possible to have a nested Grid in Haystack
# def _kind_in_dict_to_str(d_in: dict[str, Any]) -> dict[str, Any]:
#     d = d_in.copy()
#     # should I make a copy here?
#     for key in d.keys():
#         if isinstance(d[key], Number):
#             if d[key].unit is not None:
#                 d[key] = {
#                     "_kind": "number",
#                     "val": str(d[key].val),
#                     "unit": d[key].unit,
#                 }
#             else:
#                 d[key] = {"_kind": "number", "val": str(d[key].val)}
#         elif isinstance(d[key], Marker):
#             d[key] = {"_kind": "marker"}
#         elif isinstance(d[key], Remove):
#             d[key] = {"_kind": "remove"}
#         elif isinstance(d[key], NA):
#             d[key] = {"_kind": "na"}
#         elif isinstance(d[key], Ref):
#             if d[key].dis is not None:
#                 d[key] = {"_kind": "ref", "val": d[key].val, "dis": d[key].dis}
#             else:
#                 d[key] = {"_kind": "ref", "val": d[key].val}
#         elif isinstance(d[key], Date):
#             d[key] = {"_kind": "date", "val": str(d[key])}
#         elif isinstance(d[key], Time):
#             d[key] = {"_kind": "time", "val": str(d[key])}
#         elif isinstance(d[key], DateTime):
#             if d[key].tz is not None:
#                 d[key] = {"_kind": "dateTime", "val": str(d[key].val), "tz": d[key].tz}
#             else:
#                 d[key] = {"_kind": "dateTime", "val": str(d[key].val)}
#         elif isinstance(d[key], Uri):
#             d[key] = {"_kind": "uri", "val": d[key].val}
#         elif isinstance(d[key], Coordinate):
#             d[key] = {"_kind": "coord", "lat": d[key].lat, "lng": d[key].lng}
#         elif isinstance(d[key], XStr):
#             d[key] = {"_kind": "xstr", "type": d[key].type, "val": d[key].val}
#         elif isinstance(d[key], Symbol):
#             d[key] = {"_kind": "symbol", "val": d[key].val}
#     return d


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


# TODO :  Support pandas without requiring pandas dependency
def grid_to_pandas(g: kinds.Grid) -> pd.DataFrame:
    # initialize the pandas df
    col_names = [col_grid["name"] for col_grid in g.cols]
    df = pd.DataFrame(data=g.rows, columns=col_names)

    # find a map for column names - we want something more readable
    name_map = {}
    for col in g.cols:
        if col["name"] == "ts":
            name_map["ts"] = "Timestamp"

        if "meta" in col.keys():
            if "id" in col["meta"].keys():
                name_map[col["name"]] = col["meta"]["id"].dis

    # rename the columns in the df to be easier to read
    df = df.rename(columns=name_map)

    # convert kind dataclass objects to types supported by Pandas
    for col in df.columns:
        x = df[col].iloc[0]
        if isinstance(x, kinds.DateTime):
            df[col] = df[col].apply(lambda x: x.val)  # .astype(datetime)
        if isinstance(x, kinds.Number):
            df[col] = df[col].apply(lambda x: x.val).astype(float)

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


def _parse_number(d: dict[str, str]) -> kinds.Number:
    unit = d.get("unit", None)

    try:
        return kinds.Number(float(d["val"]), unit)
    except KeyError:
        logger.debug(
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise
    except ValueError:
        logger.debug(f"Unable to parse the 'val' key's value into a float:\n{d}")
        raise


def _parse_marker(d: dict[str, str]) -> kinds.Marker:
    return kinds.Marker()


def _parse_remove(d: dict[str, str]) -> kinds.Remove:
    return kinds.Remove()


def _parse_na(d: dict[str, str]) -> kinds.NA:
    return kinds.NA()


def _parse_ref(d: dict[str, str]) -> kinds.Ref:
    try:
        dis = d.get("dis", None)
        return kinds.Ref(d["val"], dis)
    except KeyError:
        logger.debug(
            f"Received this input which did not have the expected 'val' key:\n{d}"
        )
        raise


def _parse_date(d: dict[str, str]):
    try:
        return kinds.Date(date.fromisoformat(d["val"]))
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
        return kinds.Time(time.fromisoformat(d["val"]))
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
        return kinds.DateTime(dt, haystack_tz)
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
    return kinds.Uri(d["val"])


def _parse_coord(d: dict[str, str]):
    lat = float(d["lat"])
    lng = float(d["lng"])
    return kinds.Coordinate(lat, lng)


def _parse_xstr(d: dict[str, str]):
    return kinds.XStr(d["type"], d["val"])


def _parse_symbol(d: dict[str, str]):
    return kinds.Symbol(d["val"])
