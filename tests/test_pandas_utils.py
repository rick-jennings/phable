from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pytest

from phable.pandas_utils import his_long_to_wide
from tests.conftest import TS_NOW


def _table_to_long_df(table: pa.Table) -> pd.DataFrame:
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    unique_ids = sorted(df["id"].unique())
    df["id"] = df["id"].astype(
        pd.CategoricalDtype(categories=unique_ids, ordered=False)
    )
    return df


@pytest.mark.parametrize(
    "columns,expected_col",
    [
        (None, "point1"),
        ({"point1": "val"}, "val"),
    ],
    ids=["default_columns", "mapped_columns"],
)
def test_single_pt_long_to_wide(
    single_pt_his_table: pa.Table,
    columns: dict[str, str] | None,
    expected_col: str,
) -> None:
    df_wide = his_long_to_wide(_table_to_long_df(single_pt_his_table), columns)

    expected = pd.DataFrame(
        {
            "ts": pd.array(
                [TS_NOW - timedelta(seconds=30), TS_NOW],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="America/New_York")),
            ),
            expected_col: pd.array([72.2, 76.3], dtype=pd.ArrowDtype(pa.float64())),
        },
    ).set_index("ts")
    pd.testing.assert_frame_equal(df_wide, expected, check_column_type=False)


@pytest.mark.parametrize(
    "columns,pt1_col,pt2_col,pt3_col",
    [
        (None, "point1", "point2", "point3"),
        ({"point1": "v0", "point2": "v1", "point3": "v2"}, "v0", "v1", "v2"),
    ],
    ids=["default_columns", "mapped_columns"],
)
def test_multi_pt_long_to_wide(
    multi_pt_his_table: pa.Table,
    columns: dict[str, str] | None,
    pt1_col: str,
    pt2_col: str,
    pt3_col: str,
) -> None:
    df_wide = his_long_to_wide(_table_to_long_df(multi_pt_his_table), columns)

    expected = pd.DataFrame(
        {
            "ts": pd.array(
                [
                    TS_NOW - timedelta(seconds=60),
                    TS_NOW - timedelta(seconds=30),
                    TS_NOW,
                ],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="America/New_York")),
            ),
            pt1_col: pd.array([None, None, 76.3], dtype=pd.ArrowDtype(pa.float64())),
            pt2_col: pd.array(
                ["available", None, None], dtype=pd.ArrowDtype(pa.string())
            ),
            pt3_col: pd.array([True, False, None], dtype=pd.ArrowDtype(pa.bool_())),
        },
    ).set_index("ts")
    pd.testing.assert_frame_equal(df_wide, expected, check_like=True)


def test_all_na_point_omitted(his_table_all_na_for_a_point: pa.Table) -> None:
    df_wide = his_long_to_wide(_table_to_long_df(his_table_all_na_for_a_point))

    expected = pd.DataFrame(
        {
            "ts": pd.array(
                [pd.Timestamp("2024-01-01 12:00:00", tz="America/New_York")],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="America/New_York")),
            ),
            "point1": pd.array([1.0], dtype=pd.ArrowDtype(pa.float64())),
        }
    ).set_index("ts")
    pd.testing.assert_frame_equal(df_wide, expected)


def test_invalid_df_raises_error() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="DataFrame is missing required columns"):
        his_long_to_wide(df)
