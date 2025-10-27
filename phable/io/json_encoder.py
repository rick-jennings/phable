from __future__ import annotations

import json
from datetime import date, datetime, time
from typing import Any

from phable.io.ph_encoder import PhEncoder
from phable.kinds import (
    NA,
    Coord,
    Grid,
    Marker,
    Number,
    PhKind,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
)


class JsonEncoder(PhEncoder):
    def encode(self, data: PhKind) -> bytes:
        return json.dumps(_kind_to_json(data)).encode()

    @staticmethod
    def to_dict(data: PhKind) -> dict[str, Any]:
        return _kind_to_json(data)


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
            return {
                "_kind": "xstr",
                "type": kind.type,
                "val": kind.val,
            }
        case dict():
            return _dict_to_json(kind)
        case list():
            return [_kind_to_json(x) for x in kind]
        case Grid():
            return _grid_to_json(kind)
        case _:
            raise ValueError(
                f"Unable to parse input {kind} with Python type {type(kind)} into JSON"
            )


def _grid_to_json(grid: Grid) -> dict[str, Any]:
    return {
        "_kind": "grid",
        "meta": _kind_to_json(grid.meta),
        "cols": _kind_to_json(grid.cols),
        "rows": _kind_to_json(grid.rows),
    }


def _dict_to_json(row: dict[str, Any]) -> dict[str, Any]:
    parsed_row: dict[str, str | dict[str, str]] = {}
    for key in row.keys():
        parsed_row[key] = _kind_to_json(row[key])

    return parsed_row


def _number_to_json(num: Number) -> float | dict[str, str | float]:
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
