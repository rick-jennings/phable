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

from phable.kinds import NA, Grid, GridCol, Number, Ref
from phable.pandas_utils import his_long_to_wide as pandas_long_to_wide
from phable.polar_utils import his_long_to_wide as polars_long_to_wide
from tests.conftest import TS_NOW


@dataclass
class LongToWideAdapter:
    to_df: Callable[[Grid], Any]
    his_long_to_wide: Callable[..., Any]
    assert_equal: Callable[[Any, Any], None]
    from_arrow_wide: Callable[[pa.Table], Any]


def _pandas_from_arrow_wide(table: pa.Table) -> pd.DataFrame:
    return table.to_pandas(types_mapper=pd.ArrowDtype).set_index("ts")


ADAPTERS: dict[str, LongToWideAdapter] = {
    "pandas": LongToWideAdapter(
        to_df=lambda grid: grid.to_pandas(),
        his_long_to_wide=pandas_long_to_wide,
        assert_equal=lambda a, e: pandas_assert_frame_equal(
            a, e, check_column_type=False, check_like=True
        ),
        from_arrow_wide=_pandas_from_arrow_wide,
    ),
    "polars": LongToWideAdapter(
        to_df=lambda grid: grid.to_polars(),
        his_long_to_wide=polars_long_to_wide,
        assert_equal=lambda a, e: polars_assert_frame_equal(
            a, e, check_column_order=False
        ),
        from_arrow_wide=pl.from_arrow,
    ),
}


@pytest.fixture(
    params=list(ADAPTERS.values()),
    ids=list(
        ADAPTERS.keys()
    ),  # Makes test names show "pandas"/"polars" instead of "adapter0"/"adapter1"
    scope="module",
)
def adapter(request) -> Generator[LongToWideAdapter, None, None]:
    yield request.param


@pytest.mark.parametrize(
    "columns,expected_col",
    [
        (None, "point1"),
        ({"point1": "val"}, "val"),
    ],
    ids=["default_columns", "mapped_columns"],
)
def test_single_pt_long_to_wide(
    adapter: LongToWideAdapter,
    single_pt_his_grid: Grid,
    columns: dict[str, str] | None,
    expected_col: str,
) -> None:
    df_wide = adapter.his_long_to_wide(adapter.to_df(single_pt_his_grid), columns)
    expected_table = pa.table(
        {
            "ts": pa.array(
                [TS_NOW - timedelta(seconds=30), TS_NOW],
                type=pa.timestamp("us", tz="America/New_York"),
            ),
            expected_col: pa.array([72.2, 76.3], type=pa.float64()),
        }
    )
    adapter.assert_equal(df_wide, adapter.from_arrow_wide(expected_table))


@pytest.mark.parametrize(
    "columns,pt1_col,pt2_col,pt3_col",
    [
        (None, "point1", "point2", "point3"),
        ({"point1": "v0", "point2": "v1", "point3": "v2"}, "v0", "v1", "v2"),
    ],
    ids=["default_columns", "mapped_columns"],
)
def test_multi_pt_long_to_wide(
    adapter: LongToWideAdapter,
    multi_pt_his_grid: Grid,
    columns: dict[str, str] | None,
    pt1_col: str,
    pt2_col: str,
    pt3_col: str,
) -> None:
    df_wide = adapter.his_long_to_wide(adapter.to_df(multi_pt_his_grid), columns)
    expected_table = pa.table(
        {
            "ts": pa.array(
                [
                    TS_NOW - timedelta(seconds=60),
                    TS_NOW - timedelta(seconds=30),
                    TS_NOW,
                ],
                type=pa.timestamp("us", tz="America/New_York"),
            ),
            pt1_col: pa.array([None, None, 76.3], type=pa.float64()),
            pt2_col: pa.array(["available", None, None], type=pa.string()),
            pt3_col: pa.array([True, False, None], type=pa.bool_()),
        }
    )
    adapter.assert_equal(df_wide, adapter.from_arrow_wide(expected_table))


def test_all_na_point_omitted(adapter: LongToWideAdapter) -> None:
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    grid = Grid(
        {"ver": "3.0", "hisStart": ts - timedelta(hours=1), "hisEnd": ts},
        [
            GridCol("ts"),
            GridCol("v0", {"id": Ref("point1"), "kind": "Number"}),
            GridCol("v1", {"id": Ref("point2"), "kind": "Number"}),
        ],
        [{"ts": ts, "v0": Number(1.0, "kW"), "v1": NA()}],
    )
    df_wide = adapter.his_long_to_wide(adapter.to_df(grid))
    expected_table = pa.table(
        {
            "ts": pa.array([ts], type=pa.timestamp("us", tz="America/New_York")),
            "point1": pa.array([1.0], type=pa.float64()),
        }
    )
    adapter.assert_equal(df_wide, adapter.from_arrow_wide(expected_table))


def test_invalid_pandas_df_raises_error() -> None:
    with pytest.raises(ValueError, match="DataFrame is missing required columns"):
        pandas_long_to_wide(pd.DataFrame({"a": [1, 2, 3]}))


def test_invalid_polars_df_raises_error() -> None:
    with pytest.raises(ValueError, match="DataFrame is missing required columns"):
        polars_long_to_wide(pl.DataFrame({"a": [1, 2, 3]}))
