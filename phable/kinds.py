from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, getcontext
from typing import Any


class Marker:
    """`Marker` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#marker). `Marker` is a
    singleton used to create "label" tags.

    **Example:**
    ```python
    from phable.kinds import Marker

    meter_equip = {"meter": Marker(), "equip": Marker()}
    ```
    """

    __instance = None

    def __new__(cls):
        if Marker.__instance is None:
            Marker.__instance = object.__new__(cls)
        return Marker.__instance

    def __str__(self):
        return "\u2713"


class NA:
    """`NA` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#na). `NA` is a
    singleton to indicate a data value that is not available. In Project Haystack it is
    most often used in historized data to indicate a timestamp sample is in error.
    """

    __instance = None

    def __new__(cls):
        if NA.__instance is None:
            NA.__instance = object.__new__(cls)
        return NA.__instance

    def __str__(self):
        return "NA"


class Remove:
    """`Remove` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#remove). `Remove` is a
    singleton used in a `dict` to indicate removal of a tag.
    """

    __instance = None

    def __new__(cls):
        if Remove.__instance is None:
            Remove.__instance = object.__new__(cls)
        return Remove.__instance

    def __str__(self):
        return "remove"


@dataclass(frozen=True, slots=True)
class Number:
    """`Number` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#number).

    Parameters:
        val: Integer or floating point value.
        unit:
            Optional unit of measurement defined in Project Haystack's standard unit
            database [here](https://project-haystack.org/doc/docHaystack/Units).

            **Note**: Phable does not validate a defined unit at this time.
    """

    val: int | float
    unit: str | None = None

    def __str__(self):
        if self.unit is not None:
            return f"{self.val}{self.unit}"
        else:
            return f"{self.val}"


@dataclass(frozen=True, slots=True)
class Uri:
    """`Uri` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#uri).

    **Example:**
    ```python
    from phable.kinds import Uri

    uri = Uri("http://project-haystack.org/")
    ```

    Parameters:
        val:
            Universal Resource Identifier according to
            [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
    """

    val: str

    def __str__(self):
        return self.val


@dataclass(frozen=True, slots=True)
class Ref:
    """`Ref` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#ref).

    Parameters:
        val: Unique identifier for an entity.
        dis: Optional human display name.
    """

    val: str
    dis: str | None = None

    def __str__(self) -> str:
        if self.dis is not None:
            return self.dis
        else:
            return f"@{self.val}"


@dataclass(frozen=True, slots=True)
class Symbol:
    """`Symbol` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#symbol).

    Parameters:
        val:
            [def](https://project-haystack.org/doc/docHaystack/Defs) identifier.
            Consists of only ASCII letters, digits, underbar, colon, dash, period, or
            tilde.
    """

    val: str

    def __str__(self):
        return f"^{self.val}"


@dataclass(frozen=True, slots=True)
class Coord:
    """`Coord` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#coord).

    Parameters:
        lat:
            Latitude represented in
            [decimal degrees](https://en.wikipedia.org/wiki/Decimal_degrees).
        lng:
            Longitude represented in
            [decimal degrees](https://en.wikipedia.org/wiki/Decimal_degrees).
    """

    lat: Decimal
    lng: Decimal

    def __str__(self):
        getcontext().prec = 6
        return f"C({self.lat}, {self.lng})"


@dataclass(frozen=True, slots=True)
class XStr:
    """`XStr` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#xstr).

    Parameters:
        type:
            Type name that follows Project Haystack's
            [tag naming](https://project-haystack.org/doc/docHaystack/Kinds#names)
            rules, except it must start with an ASCII uppercase letter (A-Z).
        val: String encoded value.
    """

    type: str
    val: str

    def __str__(self):
        return f"({self.type}, {self.val})"


@dataclass(frozen=True, slots=True)
class Grid:
    """`Grid` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#grid).

    Parameters:
        meta: Metadata for the entire `Grid`.
        cols: Metadata for columns within the `Grid`.
        rows: Row data for `Grid`.
    """

    meta: dict[str, Any]
    cols: list[dict[str, Any]]
    rows: list[dict[str, Any]]

    def __str__(self):
        return "Haystack Grid"

    def to_pandas(self):
        """Converts a `Grid` into a Pandas DataFrame.

        Requires Phable's optional Pandas dependency to be installed.

        **Note:** This method is experimental and subject to change.
        """
        from phable.parsers.pandas import grid_to_pandas

        return grid_to_pandas(self)

    @staticmethod
    def to_grid(
        rows: dict[str, Any] | list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
    ) -> Grid:
        """Creates a `Grid` using row data and optional metadata.

        Parameters:
            rows: Row data for `Grid`.
            meta: Optional metadata for the entire `Grid`.
        """
        if isinstance(rows, dict):
            rows = [rows]

        # might be able to find a nicer way to do this
        col_names: list[str] = []
        for row in rows:
            for col_name in row.keys():
                if col_name not in col_names:
                    col_names.append(col_name)

        cols = [{"name": name} for name in col_names]

        grid_meta = {"ver": "3.0"}

        if meta is not None:
            grid_meta = grid_meta | meta

        return Grid(meta=grid_meta, cols=cols, rows=rows)


@dataclass(frozen=True, slots=True)
class DateRange:
    """`DateRange` data type, defined by `Phable`, describes a time range using dates.

    **Note:** Project Haystack does not define a kind for `DateRange`.

    Parameters:
        start: Midnight of the start date (inclusive) for the range.
        end: Midnight of the end date (exclusive) for the range.
    """

    start: date
    end: date

    def __str__(self):
        return self.start.isoformat() + "," + self.end.isoformat()


@dataclass(frozen=True, slots=True)
class DateTimeRange:
    """`DateTimeRange` data type, defined by `Phable`, describes a time range using
    date, time, and timezone information.

    **Note:** Project Haystack does not define a kind for `DateTimeRange`.

    Parameters:
        start: Start timestamp (inclusive) which is timezone aware.
        end:
            Optional end timestamp (exclusive) which is timezone aware. If end is
            undefined, then assume end to be when the last data value was recorded.
    """

    start: datetime
    end: datetime | None = None

    def __str__(self):
        if self.end is None:
            return _to_haystack_datetime(self.start)
        else:
            return (
                _to_haystack_datetime(self.start)
                + ","
                + _to_haystack_datetime(self.end)
            )


def _to_haystack_datetime(x: datetime) -> str:
    iana_tz = str(x.tzinfo)
    if "/" in iana_tz:
        haystack_tz = iana_tz.split("/")[-1]
    else:
        haystack_tz = iana_tz

    if x.microsecond == 0:
        dt = x.isoformat(timespec="seconds")
    else:
        dt = x.isoformat(timespec="milliseconds")

    return f"{dt} {haystack_tz}"
