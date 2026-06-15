from __future__ import annotations

import json as _json
from datetime import date, datetime, time
from decimal import Decimal, getcontext
from typing import Any

from phable.io.ph_codec import PhCodec
from phable.io.ph_tz import _haystack_to_iana_tz, _tz_iana_to_haystack
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


class JsonCodec(PhCodec):
    media_type = "application/json"

    def to_str(self, data: PhKind) -> str:
        return _json.dumps(_kind_to_json(data))

    def from_str(self, data: str) -> PhKind:
        return _parse_val(_json.loads(data))

    def to_dict(self, data: PhKind) -> dict[str, Any]:
        return _kind_to_json(data)  # ty: ignore [invalid-return-type]

    def from_dict(self, data: dict[str, Any]) -> PhKind:
        return _parse_val(data)


# ── Encoding ──────────────────────────────────────────────────────────────────


def _kind_to_json(kind: Any) -> float | dict[str, Any] | list[dict[str, Any]]:
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
        case GridCol():
            return _grid_col_to_json(kind)
        case dict():
            return {key: _kind_to_json(val) for key, val in kind.items()}
        case list():
            return [_kind_to_json(x) for x in kind]  # ty: ignore [invalid-return-type]
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


def _number_to_json(num: Number) -> float | dict[str, str | float]:
    if num.unit is None:
        return num.val
    return {"_kind": "number", "val": num.val, "unit": num.unit}


def _datetime_to_json(dt: datetime) -> dict[str, str]:
    return {
        "_kind": "dateTime",
        "val": dt.isoformat(),
        "tz": _tz_iana_to_haystack(str(dt.tzinfo)),
    }


def _ref_to_json(ref: Ref) -> dict[str, str]:
    d: dict[str, str] = {"_kind": "ref", "val": ref.val}
    if ref.dis is not None:
        d["dis"] = ref.dis
    return d


def _grid_col_to_json(col: GridCol) -> dict[str, Any]:
    result: dict[str, Any] = {"name": col.name}
    if col.meta:
        result["meta"] = _kind_to_json(col.meta)
    return result


# ── Decoding ──────────────────────────────────────────────────────────────────


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
            raise ValueError(f"Cannot parse value {value!r} of type {type(value).__name__}")


def _to_kind(d: dict[str, str]) -> PhKind:
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


def _parse_grid(json_data: dict[str, Any]) -> Grid:
    dict_data = _parse_dict(json_data)
    cols = [_parse_grid_col(col) for col in dict_data["cols"]]
    return Grid(meta=dict_data["meta"], cols=cols, rows=dict_data["rows"])


def _parse_dict(value_dict: dict[str, Any]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for key, value in value_dict.items():
        if key == "_kind" and value == "dict":
            continue
        parsed[key] = _parse_val(value)
    return parsed


def _parse_list(value_list: list[Any]) -> list[Any]:
    return [_parse_val(x) for x in value_list]


def _parse_number(d: dict[str, str]) -> Number:
    return Number(float(d["val"]), d.get("unit", None))


def _parse_marker(d: dict[str, str]) -> Marker:
    return Marker()


def _parse_remove(d: dict[str, str]) -> Remove:
    return Remove()


def _parse_na(d: dict[str, str]) -> NA:
    return NA()


def _parse_ref(d: dict[str, str]) -> Ref:
    return Ref(d["val"], d.get("dis", None))


def _parse_date(d: dict[str, str]) -> date:
    return date.fromisoformat(d["val"])


def _parse_time(d: dict[str, str]) -> time:
    return time.fromisoformat(d["val"])


def _parse_date_time(d: dict[str, str]) -> datetime:
    return datetime.fromisoformat(d["val"]).astimezone(_haystack_to_iana_tz(d["tz"]))


def _parse_uri(d: dict[str, str]) -> Uri:
    return Uri(d["val"])


def _parse_coord(d: dict[str, str]) -> Coord:
    getcontext().prec = 6
    return Coord(Decimal(d["lat"]), Decimal(d["lng"]))


def _parse_xstr(d: dict[str, str]) -> XStr:
    return XStr(d["type"], d["val"])


def _parse_symbol(d: dict[str, str]) -> Symbol:
    return Symbol(d["val"])


def _parse_grid_col(d: dict[str, Any]) -> GridCol:
    return GridCol(d["name"], d.get("meta"))
