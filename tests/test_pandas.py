from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from phable.kinds import Grid, Number, Ref, Uri
from phable.parsers.pandas import (
    HaystackHisGridUnitMismatchError,
    _is_his_grid,
    to_pandas,
)


def test_to_pandas_df_attributes() -> None:
    # case 1
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0", "id": Ref("1234", "foo kW")}

    cols = [{"name": "ts"}, {"name": "val"}]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "val": Number(72.2, "kW"),
        },
        {
            "ts": ts_now,
            "val": Number(76.3, "kW"),
        },
    ]

    his_grid = Grid(meta, cols, rows)
    df = to_pandas(his_grid)

    assert df.attrs["meta"] == meta
    assert df.attrs["cols"] == cols

    # verify we made a copy of meta and cols
    meta["test"] = "123"
    cols.append({"test": "123"})

    assert df.attrs["meta"] != meta
    assert df.attrs["cols"] != cols

    # case 2
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": Ref("1234", "foo1 kW")}},
        {"name": "v1", "meta": {"id": Ref("2345", "foo2 W")}},
    ]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2, "kW"),
            "v1": Number(76.3, "W"),
        },
        {"ts": ts_now, "v0": Number(76.3, "kW"), "v1": Number(72.2, "W")},
    ]

    his_grid = Grid(meta, cols, rows)
    df = to_pandas(his_grid)

    assert df.attrs["meta"] == meta
    assert df.attrs["cols"] == cols

    # verify we made a copy of meta and cols
    meta["test"] = "123"
    cols.append({"test": "123"})

    assert df.attrs["meta"] != meta
    assert df.attrs["cols"] != cols


def test_haystack_his_grid_with_single_col_unit_mismatch_error() -> None:
    with pytest.raises(HaystackHisGridUnitMismatchError):
        ts_now = datetime.now(ZoneInfo("America/New_York"))
        meta = {"ver": "3.0", "id": Ref("1234", "foo W")}
        cols = [{"name": "ts"}, {"name": "val"}]
        rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "val": Number(72.2, "W"),
            },
            {
                "ts": ts_now,
                "val": Number(76.3, "kW"),
            },
        ]

        his_grid = Grid(meta, cols, rows)
        to_pandas(his_grid)

    with pytest.raises(HaystackHisGridUnitMismatchError):
        ts_now = datetime.now(ZoneInfo("America/New_York"))
        meta = {"ver": "3.0", "id": Ref("1234", "foo kW")}
        cols = [{"name": "ts"}, {"name": "val"}]
        rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "val": Number(72.2, "W"),
            },
            {
                "ts": ts_now,
                "val": Number(76.3, "kW"),
            },
        ]

        his_grid = Grid(meta, cols, rows)
        to_pandas(his_grid)


def test_single_col_his_grid_to_pandas() -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0", "id": Ref("1234", "foo kW")}
    cols = [{"name": "ts"}, {"name": "val"}]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "val": Number(72.2, "kW"),
        },
        {
            "ts": ts_now,
            "val": Number(76.3, "kW"),
        },
    ]

    his_grid = Grid(meta, cols, rows)
    df = to_pandas(his_grid)

    assert df.loc[ts_now]["foo kW"] == 76.3
    assert df.loc[ts_now - timedelta(seconds=30)]["foo kW"] == 72.2


def test_haystack_his_grid_with_multi_col_unit_mismatch_error() -> None:
    with pytest.raises(HaystackHisGridUnitMismatchError):
        ts_now = datetime.now(ZoneInfo("America/New_York"))
        meta = {"ver": "3.0"}
        cols = [
            {"name": "ts"},
            {"name": "v0", "meta": {"id": Ref("1234", "foo1 kW")}},
            {"name": "v1", "meta": {"id": Ref("2345", "foo2 W")}},
        ]
        rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "v0": Number(72.2, "kW"),
                "v1": Number(76.3, "W"),
            },
            {"ts": ts_now, "v0": Number(76.3, "kW"), "v1": Number(72.2, "kW")},
        ]

        his_grid = Grid(meta, cols, rows)
        to_pandas(his_grid)


def test_multi_col_his_grid_to_pandas() -> None:
    # test 1
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": Ref("1234", "foo1 kW")}},
        {"name": "v1", "meta": {"id": Ref("2345", "foo2 kW")}},
    ]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2, "kW"),
            "v1": Number(76.3, "kW"),
        },
        {"ts": ts_now, "v0": Number(76.3, "kW"), "v1": Number(72.2, "kW")},
    ]

    his_grid = Grid(meta, cols, rows)
    df = to_pandas(his_grid)

    assert df.loc[ts_now - timedelta(seconds=30)]["foo1 kW"] == 72.2
    assert df.loc[ts_now - timedelta(seconds=30)]["foo2 kW"] == 76.3

    assert df.loc[ts_now]["foo1 kW"] == 76.3
    assert df.loc[ts_now]["foo2 kW"] == 72.2

    # test 2
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": Ref("1234", "foo1 kW")}},
        {"name": "v1", "meta": {"id": Ref("2345", "foo2 W")}},
    ]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2, "kW"),
            "v1": Number(76.3, "W"),
        },
        {"ts": ts_now, "v0": Number(76.3, "kW"), "v1": Number(72.2, "W")},
    ]

    his_grid = Grid(meta, cols, rows)
    df = to_pandas(his_grid)

    assert df.loc[ts_now - timedelta(seconds=30)]["foo1 kW"] == 72.2
    assert df.loc[ts_now - timedelta(seconds=30)]["foo2 W"] == 76.3

    assert df.loc[ts_now]["foo1 kW"] == 76.3
    assert df.loc[ts_now]["foo2 W"] == 72.2


def test_grid_to_pandas() -> None:
    server_time = datetime(
        2021, 5, 31, 11, 23, 23, tzinfo=ZoneInfo("America/New_York")
    )

    meta = {"ver": "3.0"}
    cols = [
        {"name": "haystackVersion"},
        {"name": "tz"},
        {"name": "serverName"},
        {"name": "serverTime"},
        {"name": "serverBootTime"},
        {"name": "productName"},
        {"name": "productUri"},
        {"name": "productVersion"},
        {"name": "vendorName"},
        {"name": "vendorUri"},
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
    df = to_pandas(grid)

    assert df.iloc[0]["productName"] == "Acme Haystack Server"
    assert df.iloc[0]["tz"] == "New_York"
    assert df.iloc[0]["haystackVersion"] == "4.0"
    assert df.iloc[0]["vendorUri"] == Uri("http://acme.com/")
    assert df.iloc[0]["serverTime"] == server_time


def test__is_his_grid_true() -> None:
    ts_now = datetime.now()
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": Ref("a1")}},
        {"name": "v1", "meta": {"id": Ref("a2")}},
    ]
    rows = [
        {
            "ts": ts_now,
            "v0": Number(72.2),
            "v1": Number(76.3),
        },
        {"ts": ts_now, "v0": Number(76.3), "v1": Number(72.2)},
    ]

    his_grid = Grid(meta, cols, rows)
    assert _is_his_grid(his_grid) is True


def test__is_his_grid_false() -> None:
    his_grid = Grid(meta={}, cols=[], rows=[])
    assert _is_his_grid(his_grid) is False
