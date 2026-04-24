from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Generator
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal as pandas_assert_frame_equal
from polars.testing import assert_frame_equal as polars_assert_frame_equal

from phable.kinds import Grid, GridCol, Number, Ref


@dataclass
class DataFrameAdapter:
    """Adapter for converting between Grid and dataframe types."""

    to_df: Callable[[Grid], Any]
    to_df_col_names_as_ids: Callable[[Grid], Any]
    from_arrow: Callable[[pa.Table], Any]
    assert_equal: Callable[[Any, Any], None]
    get_ts_timezone: Callable[[Any], str]
    get_unique_ids: Callable[[Any], list[str]]


def _pandas_from_arrow(table):
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    unique_ids = sorted(df["id"].unique())
    df["id"] = df["id"].astype(
        pd.CategoricalDtype(categories=unique_ids, ordered=False)
    )
    return df


DF_ADAPTERS: dict[str, DataFrameAdapter] = {
    "pandas": DataFrameAdapter(
        to_df=lambda grid: grid.to_pandas(),
        to_df_col_names_as_ids=lambda grid: grid.to_pandas(col_names_as_ids=True),
        from_arrow=_pandas_from_arrow,
        assert_equal=pandas_assert_frame_equal,
        get_ts_timezone=lambda df: str(df["ts"].dtype.pyarrow_dtype.tz),
        get_unique_ids=lambda df: sorted(df["id"].unique()),
    ),
    "polars": DataFrameAdapter(
        to_df=lambda grid: grid.to_polars(),
        to_df_col_names_as_ids=lambda grid: grid.to_polars(col_names_as_ids=True),
        from_arrow=pl.from_arrow,
        assert_equal=polars_assert_frame_equal,
        get_ts_timezone=lambda df: df["ts"].dtype.time_zone,
        get_unique_ids=lambda df: sorted(df["id"].unique().to_list()),
    ),
}


@pytest.fixture(
    params=list(DF_ADAPTERS.values()),
    ids=list(
        DF_ADAPTERS.keys()
    ),  # Makes test names show "pandas"/"polars" instead of "df_adapter0"/"df_adapter1"
    scope="module",
)
def df_adapter(request) -> Generator[DataFrameAdapter, None, None]:
    """Parametrized fixture that provides dataframe adapters for pandas and polars."""
    yield request.param


def test_single_pt_his_to_dataframe(
    df_adapter: DataFrameAdapter,
    single_pt_his_grid: Grid,
    single_pt_his_table: pa.Table,
) -> None:
    df = df_adapter.to_df(single_pt_his_grid)
    expected = df_adapter.from_arrow(single_pt_his_table)
    df_adapter.assert_equal(df, expected)


def test_multi_pt_his_to_dataframe(
    df_adapter: DataFrameAdapter,
    multi_pt_his_grid: Grid,
    multi_pt_his_table: pa.Table,
) -> None:
    df = df_adapter.to_df(multi_pt_his_grid)
    expected = df_adapter.from_arrow(multi_pt_his_table)
    df_adapter.assert_equal(df, expected)


def test_non_his_grid_raises_error(
    df_adapter: DataFrameAdapter, non_his_grid: Grid
) -> None:
    with pytest.raises(
        ValueError,
        match="Grid must contain time-series data with 'hisStart' in metadata.",
    ):
        df_adapter.to_df(non_his_grid)


def test_missing_id_metadata_raises_error(df_adapter: DataFrameAdapter) -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {
        "ver": "3.0",
        "hisStart": ts_now - timedelta(hours=1),
        "hisEnd": ts_now,
    }
    # Column without required 'id' metadata
    cols = [GridCol("ts"), GridCol("val")]
    rows = [{"ts": ts_now, "val": Number(72.2, "kW")}]

    grid = Grid(meta, cols, rows)

    with pytest.raises(
        ValueError,
        match="Column 'val' must have metadata with a valid 'id' of type Ref",
    ):
        df_adapter.to_df(grid)


# def test_missing_kind_metadata_raises_error(df_adapter: DataFrameAdapter) -> None:
#     ts_now = datetime.now(ZoneInfo("America/New_York"))
#     meta = {
#         "ver": "3.0",
#         "hisStart": ts_now - timedelta(hours=1),
#         "hisEnd": ts_now,
#     }
#     cols = [GridCol("ts"), GridCol("val", {"id": Ref("point1")})]
#     rows = [{"ts": ts_now, "val": Number(72.2, "kW")}]

#     grid = Grid(meta, cols, rows)

#     with pytest.raises(
#         ValueError,
#         match="Column 'val' must have metadata with a 'kind' tag",
#     ):
#         df_adapter.to_df(grid)


def test_unsupported_value_type_raises_error(df_adapter: DataFrameAdapter) -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {
        "ver": "3.0",
        "hisStart": ts_now - timedelta(hours=1),
        "hisEnd": ts_now,
    }
    cols = [GridCol("ts"), GridCol("val", {"id": Ref("point1"), "kind": "Number"})]
    # Using an unsupported type (list)
    rows = [{"ts": ts_now, "val": [1, 2, 3]}]

    grid = Grid(meta, cols, rows)

    with pytest.raises(
        ValueError,
        match="Unsupported type 'list' for column 'val'. Supported types: Number, NA, bool, or str",
    ):
        df_adapter.to_df(grid)


def test_naive_his_start_raises_error(df_adapter: DataFrameAdapter) -> None:
    # Create a naive datetime (no timezone)
    ts_now = datetime.now()
    meta = {
        "ver": "3.0",
        "hisStart": ts_now - timedelta(hours=1),  # Naive datetime
        "hisEnd": ts_now,
    }
    cols = [GridCol("ts"), GridCol("val", {"id": Ref("point1"), "kind": "Number"})]
    rows = [{"ts": ts_now, "val": Number(72.2, "kW")}]

    grid = Grid(meta, cols, rows)

    with pytest.raises(
        ValueError,
        match="'hisStart' in metadata must be timezone-aware",
    ):
        df_adapter.to_df(grid)


def test_dataframe_tz_matches_his_start(
    df_adapter: DataFrameAdapter,
) -> None:
    timezones = [
        "America/New_York",
        "Europe/London",
        "Asia/Tokyo",
        "UTC",
        "Australia/Sydney",
    ]

    for tz_name in timezones:
        tz = ZoneInfo(tz_name)
        ts_now = datetime.now(tz)
        meta = {
            "ver": "3.0",
            "hisStart": ts_now - timedelta(hours=1),
            "hisEnd": ts_now,
        }
        cols = [
            GridCol("ts"),
            GridCol("val", {"id": Ref("point1"), "unit": "kW", "kind": "Number"}),
        ]
        rows = [{"ts": ts_now, "val": Number(100.0, "kW")}]

        grid = Grid(meta, cols, rows)
        df = df_adapter.to_df(grid)

        assert df_adapter.get_ts_timezone(df) == tz_name


def test_row_ts_tz_mismatch_raises_error(
    df_adapter: DataFrameAdapter,
) -> None:
    his_start_tz = ZoneInfo("America/New_York")
    row_tz = ZoneInfo("UTC")

    ts_his_start = datetime.now(his_start_tz)
    ts_row = datetime.now(row_tz)

    meta = {
        "ver": "3.0",
        "hisStart": ts_his_start - timedelta(hours=1),
        "hisEnd": ts_his_start,
    }
    cols = [
        GridCol("ts"),
        GridCol("val", {"id": Ref("point1"), "unit": "kW", "kind": "Number"}),
    ]
    rows = [{"ts": ts_row, "val": Number(50.0, "kW")}]

    grid = Grid(meta, cols, rows)

    with pytest.raises(
        ValueError,
        match="Timestamp timezone mismatch: row timestamp has timezone 'UTC' "
        "but 'hisStart' has timezone 'America/New_York'",
    ):
        df_adapter.to_df(grid)


def test_col_names_as_ids(
    df_adapter: DataFrameAdapter,
    multi_pt_his_grid: Grid,
) -> None:
    df = df_adapter.to_df_col_names_as_ids(multi_pt_his_grid)
    assert df_adapter.get_unique_ids(df) == ["v0", "v1", "v2"]


def test_duplicate_ids_raises_error(df_adapter: DataFrameAdapter) -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {
        "ver": "3.0",
        "hisStart": ts_now - timedelta(hours=1),
        "hisEnd": ts_now,
    }
    cols = [
        GridCol("ts"),
        GridCol("v0", {"id": Ref("point1"), "kind": "Number"}),
        GridCol("v1", {"id": Ref("point1"), "kind": "Number"}),
    ]
    rows = [{"ts": ts_now, "v0": Number(1.0, "kW"), "v1": Number(2.0, "kW")}]
    grid = Grid(meta, cols, rows)

    with pytest.raises(
        ValueError,
        match="Duplicate id 'point1' found in Grid columns.",
    ):
        df_adapter.to_df(grid)


# def test_unit_mismatch_raises_error(df_adapter: DataFrameAdapter) -> None:
#     ts_now = datetime.now(ZoneInfo("America/New_York"))
#     meta = {
#         "ver": "3.0",
#         "hisStart": ts_now - timedelta(hours=1),
#         "hisEnd": ts_now,
#     }
#     cols = [GridCol("ts"), GridCol("val", {"id": Ref("point1"), "kind": "Number"})]
#     rows = [{"ts": ts_now, "val": Number(72.2, "kW")}]

#     grid = Grid(meta, cols, rows)

#     with pytest.raises(
#         ValueError,
#         match="Unit mismatch for column 'val': value has unit 'kW' "
#         "but column metadata specifies unit 'None'",
#     ):
#         df_adapter.to_df(grid)
