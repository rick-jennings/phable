from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, getcontext
from typing import Any, Mapping, Sequence, TypeAlias, cast
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
        return self.val


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
class GridCol:
    """`GridCol` defines a column in a `Grid`.

    **Example:**
    ```python
    from phable.kinds import GridCol

    # Column with metadata
    temp_col = GridCol("temp", {"unit": "°F", "dis": "Temperature"})

    # Simple column without metadata
    id_col = GridCol("id")
    ```

    Parameters:
        name: Column name following Haystack tag naming rules (lowercase start).
        meta: Optional metadata dictionary for the column (e.g., unit, display name).
    """

    name: str
    meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Grid:
    """`Grid` data type defined by Project Haystack
    [here](https://project-haystack.org/doc/docHaystack/Kinds#grid).

    Parameters:
        meta: Metadata for the entire `Grid`.
        cols: Column definitions for the `Grid`.
        rows: Row data for `Grid`.
    """

    meta: Mapping[str, Any]
    cols: Sequence[GridCol]
    rows: Sequence[Mapping[str, Any]]

    def __str__(self):
        return "Haystack Grid"

    @staticmethod
    def to_grid(
        rows: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        meta: Mapping[str, Any] | None = None,
    ) -> Grid:
        """Creates a `Grid` using row data and optional metadata.

        If parameters include history data, assumes the history rows are in
        chronological order to establish `hisStart` and `hisEnd` in `meta`.

        Parameters:
            rows: Row data for `Grid`.
            meta: Optional metadata for the entire `Grid`.
        """
        normalized_rows: Sequence[Mapping[str, Any]]
        if isinstance(rows, Mapping):
            normalized_rows = [cast(Mapping[str, Any], rows)]
        else:
            normalized_rows = rows

        # might be able to find a nicer way to do this
        col_names: list[str] = []
        for row in normalized_rows:
            for col_name in row.keys():
                if col_name not in col_names:
                    col_names.append(col_name)

        cols = [GridCol(name) for name in col_names]

        grid_meta: dict[str, Any] = {"ver": "3.0"}

        if meta is not None:
            grid_meta = grid_meta | dict(meta)

        his_start = normalized_rows[0].get("ts", None)
        his_end = normalized_rows[-1].get("ts", None)

        if his_start is not None and his_end is not None:
            grid_meta["hisStart"] = his_start
            grid_meta["hisEnd"] = his_end + timedelta(minutes=1)

        return Grid(meta=grid_meta, cols=cols, rows=normalized_rows)

    def to_pandas(self, *, col_names_as_ids: bool = False):
        """Converts time-series `Grid` to a long-format Pandas DataFrame.

        **Note:** This method is experimental and subject to change.

        **Requirements:**
        - Phable's optional Pandas dependency must be installed.
        - `Grid` must have history data (`hisStart` in Grid metadata that is timezone-aware).
        - Grid column metadata must have an `id` of type `Ref`, unless `col_names_as_ids=True`.
        - Grid row value types must be `Number`, `bool`, `str`, or `NA`.
        - Row timestamps must use the same timezone as `hisStart` in Grid metadata.

        When converting to a long-format DataFrame, history data for one or more points are combined into columns.
        Values are split into typed columns (`val_bool`, `val_str`, `val_num`) to use native DataFrame types for
        performance, since different points may have different value types. All value columns are always present for
        schema consistency to enable predictable programmatic access.

        For each DataFrame row: if the Grid value is Project Haystack's `NA`, the `val_na` column is `True` and all typed
        value columns are `None`. Otherwise, `val_na` is `None` and exactly one typed value column is populated based on
        type: `val_bool` for `bool`, `val_str` for `str`, or `val_num` for `Number`.

        | Column     | Pandas Type                  | Nullable | Description                                                                                 |
        |------------|------------------------------|----------|---------------------------------------------------------------------------------------------|
        | `id`       | `Categorical`                | No       | Point identifier from Ref (without `@` prefix), or column name when `col_names_as_ids=True` |
        | `ts`       | `timestamp[us, tz][pyarrow]` | No       | Timestamp of the reading                                                                    |
        | `val_bool` | `bool[pyarrow]`              | Yes      | Boolean value (when `kind` tag is `Bool`)                                                   |
        | `val_str`  | `string[pyarrow]`            | Yes      | String value (when `kind` tag is `Str`)                                                     |
        | `val_num`  | `double[pyarrow]`            | Yes      | Numeric value (when `kind` tag is `Number`)                                                 |
        | `val_na`   | `bool[pyarrow]`              | Yes      | `True` when value is Project Haystack's `NA`                                                |

        The resultant DataFrame is sorted by `id` and `ts`.

        Phable users are encouraged to interpolate data while in the long format dataframe using `val_na` before
        pivoting to a wide format dataframe, since pivoting loses `NA` semantics which define where
        interpolation should not occur.

        **Example:**

        ```python
        from phable.pandas_utils import his_long_to_wide

        df_long = his_grid.to_pandas()

        # if applicable, interpolate using val_na before pivoting (pivoting loses NA semantics)

        df_wide = his_long_to_wide(df_long)
        ```

        Parameters:
            col_names_as_ids:
                When `True`, column names are used as `id` values.

        Raises:
            ValueError:
                If `Grid` does not have `hisStart` in metadata, `hisStart` is not timezone-aware,
                row timestamps have a different timezone than `hisStart`, columns are missing required `id`
                metadata of type Ref (when `col_names_as_ids=False`), values are unsupported types, or two or
                more columns share the same `id` value and `col_names_as_ids=False`.
        """
        import pandas as pd
        import pyarrow as pa

        tz, data = _structure_long_format_for_df(self, col_names_as_ids)

        schema = pa.schema(
            [
                ("id", pa.dictionary(pa.int32(), pa.string())),
                ("ts", pa.timestamp("us", tz=tz.key)),
                ("val_bool", pa.bool_()),
                ("val_str", pa.string()),
                ("val_num", pa.float64()),
                ("val_na", pa.bool_()),
            ]
        )

        table = pa.Table.from_pylist(data, schema=schema)
        df = table.to_pandas(types_mapper=pd.ArrowDtype)

        unique_ids = sorted(df["id"].unique())
        df["id"] = df["id"].astype(
            pd.CategoricalDtype(categories=unique_ids, ordered=False)
        )

        return df.sort_values(["id", "ts"]).reset_index(drop=True)

    def to_polars(self, *, col_names_as_ids: bool = False):
        """Converts time-series `Grid` to a long-format Polars DataFrame.

        **Note:** This method is experimental and subject to change.

        **Requirements:**
        - Phable's optional Polars dependency must be installed.
        - `Grid` must have history data (`hisStart` in Grid metadata that is timezone-aware).
        - Grid column metadata must have an `id` of type `Ref`, unless `col_names_as_ids=True`.
        - Grid row value types must be `Number`, `bool`, `str`, or `NA`.
        - Row timestamps must use the same timezone as `hisStart` in Grid metadata.

        When converting to a long-format DataFrame, history data for one or more points are combined into columns.
        Values are split into typed columns (`val_bool`, `val_str`, `val_num`) to use native DataFrame types for
        performance, since different points may have different value types. All value columns are always present for
        schema consistency to enable predictable programmatic access.

        For each DataFrame row: if the Grid value is Project Haystack's `NA`, the `val_na` column is `True` and all typed
        value columns are `None`. Otherwise, `val_na` is `None` and exactly one typed value column is populated based on
        type: `val_bool` for `bool`, `val_str` for `str`, or `val_num` for `Number`.

        | Column     | Polars Type        | Nullable | Description                                                                                 |
        |------------|--------------------|----------|---------------------------------------------------------------------------------------------|
        | `id`       | `Categorical`      | No       | Point identifier from Ref (without `@` prefix), or column name when `col_names_as_ids=True` |
        | `ts`       | `Datetime[us, tz]` | No       | Timestamp of the reading                                                                    |
        | `val_bool` | `Boolean`          | Yes      | Boolean value (when `kind` tag is `Bool`)                                                   |
        | `val_str`  | `String`           | Yes      | String value (when `kind` tag is `Str`)                                                     |
        | `val_num`  | `Float64`          | Yes      | Numeric value (when `kind` tag is `Number`)                                                 |
        | `val_na`   | `Boolean`          | Yes      | `True` when value is Project Haystack's `NA`                                                |

        The resultant DataFrame is sorted by `id` and `ts`.

        Phable users are encouraged to interpolate data while in the long format dataframe using `val_na` before
        pivoting to a wide format dataframe, since pivoting loses `NA` semantics which define where
        interpolation should not occur.

        **Example:**

        ```python
        from phable.polar_utils import his_long_to_wide

        df_long = his_grid.to_polars()

        # if applicable, interpolate using val_na before pivoting (pivoting loses NA semantics)

        df_wide = his_long_to_wide(df_long)
        ```

        Parameters:
            col_names_as_ids:
                When `True`, column names are used as `id` values.

        Raises:
            ValueError:
                If `Grid` does not have `hisStart` in metadata, `hisStart` is not timezone-aware,
                row timestamps have a different timezone than `hisStart`, columns are missing required `id`
                metadata of type Ref (when `col_names_as_ids=False`), values are unsupported types, or two or
                more columns share the same `id` value and `col_names_as_ids=False`.
        """
        import polars as pl  # ty: ignore[unresolved-import]

        tz, data = _structure_long_format_for_df(self, col_names_as_ids)

        schema = {
            "id": pl.Categorical,
            "ts": pl.Datetime(time_unit="us", time_zone=tz.key),
            "val_bool": pl.Boolean,
            "val_str": pl.String,
            "val_num": pl.Float64,
            "val_na": pl.Boolean,
        }

        return pl.DataFrame(data=data, schema=schema).sort("id", "ts")


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


def _validate_his_grid_metadata(grid: Grid) -> None:
    if "hisStart" not in grid.meta:
        raise ValueError(
            "Grid must contain time-series data with 'hisStart' in metadata."
        )

    tz = grid.meta["hisStart"].tzinfo
    if tz is None:
        raise ValueError("'hisStart' in metadata must be timezone-aware.")

    for col in grid.cols:
        if col.name == "ts":
            continue

        if col.meta is None or not isinstance(col.meta.get("id"), Ref):
            raise ValueError(
                f"Column '{col.name}' must have metadata with a valid 'id' of type Ref."
            )

        # if col.meta.get("kind") is None:
        #     raise ValueError(
        #         f"Column '{col.name}' must have metadata with a 'kind' tag."
        #     )


def _structure_long_format_for_df(
    grid: Grid, col_names_as_ids: bool = False
) -> tuple[ZoneInfo, list[dict[str, Any]]]:
    _validate_his_grid_metadata(grid)
    tz = grid.meta["hisStart"].tzinfo

    if not col_names_as_ids:
        ids = [col.meta["id"].val for col in grid.cols if col.name != "ts"]  # type: ignore[index]
        seen: set[str] = set()
        for id_ in ids:
            if id_ in seen:
                raise ValueError(
                    f"Duplicate id '{id_}' found in Grid columns. "
                    "Consider using col_names_as_ids=True to assign unique column names instead."
                )
            seen.add(id_)

    rows = []

    for row in grid.rows:
        ts = row.get("ts")

        if not isinstance(ts, datetime):
            raise ValueError(
                f"Row timestamp must be a datetime object, got {type(ts).__name__}"
            )

        if ts.tzinfo.key != tz.key:
            raise ValueError(
                f"Timestamp timezone mismatch: row timestamp has timezone '{ts.tzinfo.key}' "
                f"but 'hisStart' has timezone '{tz.key}'. All timestamps must use the same timezone as 'hisStart'."
            )

        for col in grid.cols:
            if col.name == "ts":
                continue

            assert col.meta is not None  # for type checker

            point_id = col.name if col_names_as_ids else col.meta["id"].val
            # expected_unit = col.meta.get("unit")
            # kind = col.meta["kind"]

            raw_val = row.get(col.name)

            if raw_val is None:
                continue

            # type_to_kind = {Number: "Number", bool: "Bool", str: "Str"}
            # actual_kind = type_to_kind.get(type(raw_val))

            # if actual_kind and actual_kind != kind:
            #     raise ValueError(
            #         f"Type mismatch for column '{col.name}': value is {actual_kind} "
            #         f"but column metadata specifies kind '{kind}'."
            #     )

            if isinstance(raw_val, NA):
                val_bool = None
                val_str = None
                val_num = None
                val_na = True
            elif isinstance(raw_val, Number):
                # if expected_unit != raw_val.unit:
                #     raise ValueError(
                #         f"Unit mismatch for column '{col.name}': value has unit '{raw_val.unit}' "
                #         f"but column metadata specifies unit '{expected_unit}'."
                #     )
                val_bool = None
                val_str = None
                val_num = raw_val.val
                val_na = None
            elif isinstance(raw_val, bool):
                val_bool = raw_val
                val_str = None
                val_num = None
                val_na = None
            elif isinstance(raw_val, str):
                val_bool = None
                val_str = raw_val
                val_num = None
                val_na = None
            else:
                raise ValueError(
                    f"Unsupported type '{type(raw_val).__name__}' for column '{col.name}'. "
                    f"Supported types: Number, NA, bool, or str."
                )

            rows.append(
                {
                    "id": point_id,
                    "ts": ts,
                    "val_bool": val_bool,
                    "val_str": val_str,
                    "val_num": val_num,
                    "val_na": val_na,
                }
            )

    return tz, rows


# TODO: use Python 3.12 type instead of TypeAlias when Python 3.11 is no longer supported
PhKind: TypeAlias = (
    Marker
    | NA
    | Remove
    | bool
    | Number
    | str
    | Uri
    | Ref
    | Symbol
    | date
    | time
    | datetime
    | Coord
    | XStr
    | list
    | dict
    | Grid
    | GridCol
)
