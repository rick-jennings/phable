from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, getcontext
from typing import Any
from zoneinfo import ZoneInfo


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
        val: Floating point value.
        unit:
            Optional unit of measurement defined in Project Haystack's standard unit
            database [here](https://project-haystack.org/doc/docHaystack/Units).

            **Note**: Phable does not validate a defined unit at this time.
    """

    val: float
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

    @staticmethod
    def to_grid(
        rows: dict[str, Any] | list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
    ) -> Grid:
        """Creates a `Grid` using row data and optional metadata.

        If parameters include history data, assumes the history rows are in
        chronological order to establish `hisStart` and `hisEnd` in `meta`.

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

        his_start = rows[0].get("ts", None)
        his_end = rows[-1].get("ts", None)

        if his_start is not None and his_end is not None:
            grid_meta["hisStart"] = his_start
            grid_meta["hisEnd"] = his_end + timedelta(minutes=1)

        return Grid(meta=grid_meta, cols=cols, rows=rows)

    def get_df_meta(
        self,
    ) -> dict[str, dict[str, Any] | list[dict[str, Any]]]:
        """Gets metadata for a DataFrame describing data from a `Grid`.

        In the returned dictionary:

         - Value for `meta` key is data used in Grid's `meta` attribute.
         - Value for `cols` key is data used in Grid's `cols` attribute.

        Returns:
            Dictionary with keys `meta` and `cols`.
        """

        df_meta = {}
        df_meta["meta"] = self.meta.copy()
        df_meta["cols"] = self.cols.copy()
        return df_meta

    def to_pandas(self):
        """Converts rows in the `Grid` to a Pandas DataFrame.

        Requires Phable's optional Pandas dependency to be installed.

        For Grids with rows that do not have history data, Phable defined data types
        are passed as the `data` input to the DataFrame.

        For Grids with rows that have history data, an opinionated mashing process is
        applied to data passed to the DataFrame's `data` input:

         - Phable's `NA` objects are converted to `None`
         - Missing column values are converted to `None`
         - `Number` objects are converted to unitless `float` values

        The resultant Pandas DataFrame's data types are converted to the `pyarrow` data
        format.

        **Notes:**

         - This method is experimental and subject to change.
         - This method assumes all `Number` objects in a given column has the same unit.

        **Example:**

        ```python
        from datetime import datetime, timedelta

        from phable import Grid, Number

        ts_now = datetime.now()
        data = [
            {"ts": ts_now - timedelta(minutes=30), "val": Number(30, "kW")},
            {"ts": ts_now, "val": Number(38, "kW")},
        ]
        data = Grid.to_grid(data)
        df = data.to_pandas()
        ```
        """
        import pandas as pd

        data = _get_data_for_df(self)
        df = pd.DataFrame(data=data).convert_dtypes(dtype_backend="pyarrow")

        return df

    def to_pandas_all(self):
        """Returns a tuple:  `(Grid.get_df_meta(), Grid.to_pandas())`

        **Example:**

        ```python
        from datetime import datetime, timedelta

        from phable import Grid, Number

        ts_now = datetime.now()
        data = [
            {"ts": ts_now - timedelta(minutes=30), "val": Number(30, "kW")},
            {"ts": ts_now, "val": Number(38, "kW")},
        ]
        data = Grid.to_grid(data)
        df_meta, df = data.to_pandas_all()
        ```
        """
        return self.get_df_meta(), self.to_pandas()

    def to_polars(self):
        """Converts rows in the `Grid` to a Polars DataFrame.

        Requires Phable's optional Polars dependency to be installed.

        For Grids with rows that do not have history data, Phable defined data types
        are passed as the `data` input to the DataFrame.

        For Grids with rows that have history data, an opinionated mashing process is
        applied to data passed to the DataFrame's `data` input:

         - Phable's `NA` objects are converted to `None`
         - Missing column values are converted to `None`
         - `Number` objects are converted to unitless `float` values

        **Notes:**

         - This method is experimental and subject to change.
         - This method assumes all `Number` objects in a given column has the same unit.

        **Example:**

        ```python
        from datetime import datetime, timedelta

        from phable import Grid, Number

        ts_now = datetime.now()
        data = [
            {"ts": ts_now - timedelta(minutes=30), "val": Number(30, "kW")},
            {"ts": ts_now, "val": Number(38, "kW")},
        ]
        data = Grid.to_grid(data)
        df = data.to_polars()
        ```
        """

        import polars as pl

        data = _get_data_for_df(self)
        df = pl.DataFrame(data=data)

        return df

    def to_polars_all(self):
        """Returns a tuple:  `(Grid.get_df_meta(), Grid.to_polars())`

        **Example:**

        ```python
        from datetime import datetime, timedelta

        from phable import Grid, Number

        ts_now = datetime.now()
        data = [
            {"ts": ts_now - timedelta(minutes=30), "val": Number(30, "kW")},
            {"ts": ts_now, "val": Number(38, "kW")},
        ]
        data = Grid.to_grid(data)
        df_meta, df = data.to_polars_all()
        ```
        """
        return self.get_df_meta(), self.to_polars()


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

    `datetime` objects used for `start` and `end` must be timezone aware using
    `ZoneInfo` as a concrete implementation of the `datetime.tzinfo` abstract base
    class.

    **Example:**

    ```python
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from phable.kinds import DateTimeRange

    tzinfo = ZoneInfo("America/New_York")
    start = datetime(2024, 11, 22, 8, 19, 0, tzinfo=tzinfo)
    end = datetime(2024, 11, 22, 9, 19, 0, tzinfo=tzinfo)

    range_with_end = DateTimeRange(start, end)
    range_without_end = DateTimeRange(start)
    ```

    **Note:** Project Haystack does not define a kind for `DateTimeRange`.

    Parameters:
        start: Start timestamp (inclusive) which is timezone aware using `ZoneInfo`.
        end:
            Optional end timestamp (exclusive) which is timezone aware using
            `ZoneInfo`. If end is undefined, then assume end to be when the last data
            value was recorded.
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

    def __post_init__(self):
        start_ok = isinstance(self.start.tzinfo, ZoneInfo)
        end_ok = self.end is None

        if isinstance(self.end, datetime):
            end_ok = isinstance(self.end.tzinfo, ZoneInfo)

        if start_ok is False or end_ok is False:
            raise ValueError


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


def _get_data_for_df(grid: Grid):
    if "hisStart" in grid.meta.keys():
        data = _structure_his_data_for_df(grid)
    else:
        data = grid.rows

    return data


def _structure_his_data_for_df(grid: Grid) -> dict[str, list[Any]]:
    col_names = [col["name"] for col in grid.cols]
    data = {}
    for col_name in col_names:
        data[col_name] = []

    for row in grid.rows:
        for col_name in col_names:
            col_val = row.get(col_name, None)

            if col_val is None:
                data[col_name].append(None)
            elif isinstance(col_val, datetime):
                data[col_name].append(col_val)
            elif isinstance(col_val, NA):
                data[col_name].append(None)
            elif isinstance(col_val, Number):
                data[col_name].append(col_val.val)
            elif isinstance(col_val, bool):
                data[col_name].append(col_val)
            elif isinstance(col_val, str):
                data[col_name].append(col_val)
            else:
                raise ValueError

    return data
