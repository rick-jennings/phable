from __future__ import annotations

from datetime import datetime, timedelta

# from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from phable.kinds import NA, Grid, GridCol, Number, Ref, Uri

TS_NOW = datetime.now(ZoneInfo("America/New_York"))


@pytest.fixture(scope="module")
def one_col_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [GridCol("ts"), GridCol("val")]
    rows = [
        {
            "ts": TS_NOW - timedelta(seconds=60),
            "val": NA(),
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "val": Number(72.2, "kW"),
        },
        {
            "ts": TS_NOW,
            "val": Number(76.3, "kW"),
        },
    ]

    return Grid(meta, cols, rows)


def test_get_his_df_with_one_col(one_col_grid: Grid) -> None:
    df = one_col_grid.to_pandas()
    expected_df = pd.DataFrame(
        data={
            "ts": [
                one_col_grid.rows[0]["ts"],
                one_col_grid.rows[1]["ts"],
                one_col_grid.rows[2]["ts"],
            ],
            "val": [None, 72.2, 76.3],
        }
    ).convert_dtypes(dtype_backend="pyarrow")

    assert isinstance(df["ts"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_datetime64_any_dtype(df["ts"].dtype)
    assert isinstance(df["val"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_float_dtype(df["val"].dtype)

    assert_frame_equal(df, expected_df)


@pytest.fixture(scope="module")
def multi_col_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [
        GridCol("ts"),
        GridCol("v0"),
        GridCol("v1"),
        GridCol("v2"),
        GridCol("v3"),
        GridCol("v4"),
    ]
    rows = [
        {"ts": TS_NOW - timedelta(seconds=60), "v0": NA(), "v4": True},
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "v1": Number(72.2, "kW"),
            "v3": "available",
            "v4": NA(),
        },
        {
            "ts": TS_NOW,
            "v2": Number(76.3, "kW"),
            "v3": "occupied",
            "v4": False,
        },
    ]

    return Grid(meta, cols, rows)


def test_get_his_df_with_multi_cols(multi_col_grid: Grid) -> None:
    df = multi_col_grid.to_pandas()

    expected_df = pd.DataFrame(
        {
            "ts": [
                multi_col_grid.rows[0]["ts"],
                multi_col_grid.rows[1]["ts"],
                multi_col_grid.rows[2]["ts"],
            ],
            "v0": [None, None, None],
            "v1": [None, 72.2, None],
            "v2": [None, None, 76.3],
            "v3": [None, "available", "occupied"],
            "v4": [True, None, False],
        }
    ).convert_dtypes(dtype_backend="pyarrow")

    assert isinstance(df["ts"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_datetime64_any_dtype(df["ts"].dtype)

    assert isinstance(df["v0"].dtype, pd.ArrowDtype)

    assert isinstance(df["v1"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_float_dtype(df["v1"].dtype)

    assert isinstance(df["v2"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_float_dtype(df["v2"].dtype)

    assert isinstance(df["v3"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_string_dtype(df["v3"].dtype)

    assert isinstance(df["v4"].dtype, pd.ArrowDtype)
    assert pd.api.types.is_bool_dtype(df["v4"].dtype)

    assert_frame_equal(df, expected_df)


def test_grid_to_pandas() -> None:
    server_time = datetime(2021, 5, 31, 11, 23, 23, tzinfo=ZoneInfo("America/New_York"))

    meta = {"ver": "3.0"}
    cols = [
        GridCol("haystackVersion"),
        GridCol("tz"),
        GridCol("serverName"),
        GridCol("serverTime"),
        GridCol("serverBootTime"),
        GridCol("productName"),
        GridCol("productUri"),
        GridCol("productVersion"),
        GridCol("vendorName"),
        GridCol("vendorUri"),
    ]
    rows = [
        {
            "haystackVersion": "4.0",
            "tz": "New_York",
            "serverName": "Test Server",
            "serverTime": server_time,
            "serverBootTime": server_time,
            "productName": "Acme Haystack Server",
            "productUri": Uri("http://acme.com/haystack-server"),
            "productVersion": "1.0.30",
            "vendorName": "Acme",
            "vendorUri": Uri("http://acme.com/"),
        }
    ]

    grid = Grid(meta, cols, rows)
    df = grid.to_pandas()

    assert df.iloc[0]["productName"] == "Acme Haystack Server"
    assert df.iloc[0]["tz"] == "New_York"
    assert df.iloc[0]["haystackVersion"] == "4.0"
    assert df.iloc[0]["vendorUri"] == Uri("http://acme.com/")
    assert df.iloc[0]["serverTime"] == server_time
