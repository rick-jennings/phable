from datetime import datetime, timedelta

import pytest

from phable.kinds import Grid, Marker, Number, Ref
from phable.parsers.grid import (
    ColIdMismatchError,
    ColNumMismatchError,
    merge_pt_data_to_his_grid_cols,
)

# -------------------------------------------------------------------------
# one point
# -------------------------------------------------------------------------


@pytest.fixture
def one_pt_grid() -> Grid:
    meta = {"ver": "3.0"}
    cols = [{"name": "id"}, {"name": "A"}, {"name": "B"}]
    pt_data = Grid(
        meta, cols, [{"id": Ref("somePower"), "A": "apple", "B": "banana"}]
    )
    return pt_data


@pytest.fixture
def one_pt_his_grid() -> Grid:
    # define the his_grid
    his_end = datetime.now()
    his_start = his_end - timedelta(minutes=5)

    meta = {
        "ver": "3.0",
        "id": Ref("somePower"),
        "hisStart": his_start,
        "hisEnd": his_end,
    }
    cols = [{"name": "ts"}, {"name": "val"}]
    rows = [
        {"ts": his_start, "val": Number(12, "kW")},
        {"ts": his_end, "val": Number(24, "kW")},
    ]

    his_grid = Grid(meta, cols, rows)

    return his_grid


def test_merge_pt_data_to_his_grid_cols_with_one_pt(
    one_pt_his_grid: Grid, one_pt_grid: Grid
) -> None:
    output = merge_pt_data_to_his_grid_cols(one_pt_his_grid, one_pt_grid)
    expected_output = [
        {"name": "ts"},
        {
            "name": "val",
            "meta": {"id": Ref("somePower"), "A": "apple", "B": "banana"},
        },
    ]

    assert output == expected_output


def test_merge_pt_data_to_his_grid_cols_with_one_pt_raises_ColNumMismatchError(
    one_pt_his_grid: Grid,
) -> None:
    # define the pt_data
    meta = {"ver": "3.0"}
    cols = [{"name": "id"}, {"name": "A"}, {"name": "B"}]
    pt_data = Grid(
        meta,
        cols,
        [
            {"id": Ref("somePower1"), "A": "apple", "B": "banana"},
            {"id": Ref("somePower2"), "A": "apple", "B": "banana"},
        ],
    )

    with pytest.raises(ColNumMismatchError):
        merge_pt_data_to_his_grid_cols(one_pt_his_grid, pt_data)


def test_merge_pt_data_to_his_grid_cols_with_one_pt_raises_ColIdMismatchError(
    one_pt_his_grid: Grid,
) -> None:
    # define the pt_data
    meta = {"ver": "3.0"}
    cols = [{"name": "id"}, {"name": "A"}, {"name": "B"}]
    pt_data = Grid(
        meta,
        cols,
        [
            {"id": Ref("somePower1"), "A": "apple", "B": "banana"},
        ],
    )

    with pytest.raises(ColIdMismatchError):
        merge_pt_data_to_his_grid_cols(one_pt_his_grid, pt_data)


# -------------------------------------------------------------------------
# more than one point
# -------------------------------------------------------------------------


@pytest.fixture
def multi_pt_grid() -> Grid:
    meta = {"ver": "3.0"}
    cols = [{"name": "id"}, {"name": "dis"}]
    rows = [
        {"id": Ref("A"), "dis": "123", "test": Marker()},
        {"id": Ref("B"), "dis": "456", "test": Marker()},
        {"id": Ref("C"), "dis": "789", "test": Marker()},
    ]
    pt_data = Grid(meta, cols, rows)
    return pt_data


@pytest.fixture
def multi_pt_grid2() -> Grid:
    meta = {"ver": "3.0"}
    cols = [{"name": "id"}, {"name": "dis"}]
    rows = [
        {"id": Ref("A"), "dis": "123", "test": Marker()},
        {"id": Ref("B"), "dis": "456", "test": Marker()},
        {"id": Ref("D"), "dis": "789", "test": Marker()},
    ]
    pt_data = Grid(meta, cols, rows)
    return pt_data


@pytest.fixture
def multi_pt_his_grid() -> Grid:
    # define the his_grid
    his_end = datetime.now()
    his_start = his_end - timedelta(minutes=5)

    meta = {
        "ver": "3.0",
        "hisStart": his_start,
        "hisEnd": his_end,
    }
    cols = [
        {"name": "ts"},
        {
            "name": "v0",
            "meta": {"id": Ref("A"), "dis": "hi", "power": Marker()},
        },
        {
            "name": "v1",
            "meta": {"id": Ref("B"), "dis": "hi", "energy": Marker()},
        },
        {
            "name": "v2",
            "meta": {"id": Ref("C"), "dis": "hi", "current": Marker()},
        },
    ]
    rows = [
        {
            "ts": his_start,
            "v0": Number(12, "kW"),
            "v1": Number(14, "kW"),
            "v2": Number(16, "kW"),
        },
        {
            "ts": his_end,
            "v0": Number(20, "kW"),
            "v1": Number(22, "kW"),
            "v2": Number(24, "kW"),
        },
    ]

    his_grid = Grid(meta, cols, rows)

    return his_grid


def test_merge_pt_data_to_his_grid_cols_with_multi_pt(
    multi_pt_his_grid: Grid, multi_pt_grid: Grid
) -> None:
    output = merge_pt_data_to_his_grid_cols(multi_pt_his_grid, multi_pt_grid)
    expected_output = [
        {"name": "ts"},
        {
            "name": "v0",
            "meta": {
                "id": Ref("A"),
                "dis": "123",
                "power": Marker(),
                "test": Marker(),
            },
        },
        {
            "name": "v1",
            "meta": {
                "id": Ref("B"),
                "dis": "456",
                "energy": Marker(),
                "test": Marker(),
            },
        },
        {
            "name": "v2",
            "meta": {
                "id": Ref("C"),
                "dis": "789",
                "current": Marker(),
                "test": Marker(),
            },
        },
    ]

    assert output == expected_output


def test_merge_pt_data_to_his_cols_multi_pt_raises_ColNumMismatchError(
    multi_pt_his_grid: Grid, one_pt_grid: Grid
) -> None:
    with pytest.raises(ColNumMismatchError):
        merge_pt_data_to_his_grid_cols(multi_pt_his_grid, one_pt_grid)


def test_merge_pt_data_to_his_cols_multi_pt_raises_ColIdMismatchError(
    multi_pt_his_grid: Grid, multi_pt_grid2: Grid
) -> None:
    with pytest.raises(ColIdMismatchError):
        merge_pt_data_to_his_grid_cols(multi_pt_his_grid, multi_pt_grid2)
