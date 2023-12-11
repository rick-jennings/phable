from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from phable.client import Client
from phable.kinds import Grid, Number, Ref, Uri
from phable.parsers.pandas import (
    DuplicateColNameError,
    NotFoundError,
    UnitMismatchError,
    get_col_meta,
    grid_to_pandas,
    his_grid_to_pandas,
)


@pytest.fixture
def hc() -> Client:
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    return Client(uri, username, password)


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
    df = grid_to_pandas(his_grid)

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
    df = grid_to_pandas(his_grid)

    assert df.attrs["meta"] == meta
    assert df.attrs["cols"] == cols

    # verify we made a copy of meta and cols
    meta["test"] = "123"
    cols.append({"test": "123"})

    assert df.attrs["meta"] != meta
    assert df.attrs["cols"] != cols


def test_get_col_meta() -> None:
    col_meta1 = {
        "name": "v0",
        "meta": {
            "id": Ref(
                val="p:demo:r:2caffc8e-5a197a29",
                dis="Headquarters ElecMeter-Main kW",
            ),
            "kind": "Number",
            "unit": "kW",
        },
    }

    col_meta2 = {
        "name": "v1",
        "meta": {
            "id": Ref(
                val="p:demo:r:2caffc8e-43db8fe3",
                dis="Gaithersburg ElecMeter-Lighting kW",
            ),
            "kind": "Number",
            "unit": "kW",
        },
    }

    df_attrs = {"cols": [col_meta1, col_meta2]}
    assert (
        get_col_meta(df_attrs, "Headquarters ElecMeter-Main kW") == col_meta1
    )

    assert (
        get_col_meta(
            df_attrs,
            Ref(
                val="p:demo:r:2caffc8e-43db8fe3",
                dis="Gaithersburg ElecMeter-Lighting kW",
            ),
        )
        == col_meta2
    )


def test_get_col_meta_raises_not_found_error() -> None:
    col_meta1 = {
        "name": "v0",
        "meta": {
            "id": Ref(
                val="p:demo:r:2caffc8e-5a197a29",
                dis="Headquarters ElecMeter-Main kW",
            ),
            "kind": "Number",
            "unit": "kW",
        },
    }

    col_meta2 = {
        "name": "v1",
        "meta": {
            "id": Ref(
                val="p:demo:r:2caffc8e-43db8fe3",
                dis="Gaithersburg ElecMeter-Lighting kW",
            ),
            "kind": "Number",
            "unit": "kW",
        },
    }

    df_attrs = {"cols": [col_meta1, col_meta2]}

    with pytest.raises(NotFoundError):
        get_col_meta(df_attrs, "Hello World!")


def test_get_col_meta_raises_unit_mismatch_error(hc: Client) -> None:
    with pytest.raises(UnitMismatchError):
        with hc:
            pt_data = hc.read("power and point and equipRef->siteMeter")
            pt_data.loc[0, "unit"] = "W"
            hc.his_read(pt_data, date.today())


def test_single_col_his_grid_to_pandas() -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))

    foo = Ref("1234", "foo kW")

    pt_data = pd.DataFrame(
        [
            {"id": foo, "kind": "Number", "unit": "kW"},
        ]
    )

    meta = {"ver": "3.0", "id": foo}
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
    df = his_grid_to_pandas(his_grid, pt_data)

    assert df.loc[ts_now]["foo kW"] == 76.3
    assert df.loc[ts_now - timedelta(seconds=30)]["foo kW"] == 72.2
    assert get_col_meta(df.attrs, foo) == {
        "name": "val",
        "meta": {
            "id": foo,
            "kind": "Number",
            "unit": "kW",
        },
    }

    assert isinstance(df.attrs["meta"], dict)
    assert df.attrs["meta"]["ver"] == "3.0"


def test_multi_col_his_grid_to_pandas() -> None:
    # define refs
    foo1 = Ref("1234", "foo1 kW")
    foo2 = Ref("2345", "foo2 kW")

    pt_data = pd.DataFrame(
        [
            {"id": foo1, "kind": "Number", "unit": "kW"},
            {"id": foo2, "kind": "Number", "unit": "kW"},
        ]
    )

    # test 1
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": foo1}},
        {"name": "v1", "meta": {"id": foo2}},
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
    df = his_grid_to_pandas(his_grid, pt_data)

    assert df.loc[ts_now - timedelta(seconds=30)]["foo1 kW"] == 72.2
    assert df.loc[ts_now - timedelta(seconds=30)]["foo2 kW"] == 76.3

    assert df.loc[ts_now]["foo1 kW"] == 76.3
    assert df.loc[ts_now]["foo2 kW"] == 72.2

    # test 2
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": foo1}},
        {"name": "v1", "meta": {"id": foo2}},
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
    df = his_grid_to_pandas(his_grid, pt_data)

    assert df.loc[ts_now - timedelta(seconds=30)]["foo1 kW"] == 72.2
    assert df.loc[ts_now - timedelta(seconds=30)]["foo2 kW"] == 76.3

    assert df.loc[ts_now]["foo1 kW"] == 76.3
    assert df.loc[ts_now]["foo2 kW"] == 72.2

    assert get_col_meta(df.attrs, "foo1 kW") == {
        "name": "v0",
        "meta": {
            "id": foo1,
            "kind": "Number",
            "unit": "kW",
        },
    }

    assert get_col_meta(df.attrs, "foo2 kW") == {
        "name": "v1",
        "meta": {
            "id": foo2,
            "kind": "Number",
            "unit": "kW",
        },
    }

    assert isinstance(df.attrs["meta"], dict)
    assert df.attrs["meta"]["ver"] == "3.0"


def test_multi_col_his_grid_to_pandas_raises_duplicate_col_name_error() -> (
    None
):
    foo1 = Ref("1234", "foo1 kW")
    foo2 = Ref("2345", "foo1 kW")

    pt_data = pd.DataFrame(
        [
            {"id": foo1, "kind": "Number", "unit": "kW"},
            {"id": foo2, "kind": "Number", "unit": "kW"},
        ]
    )

    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": foo1}},
        {"name": "v1", "meta": {"id": foo2}},
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

    with pytest.raises(DuplicateColNameError):
        his_grid_to_pandas(his_grid, pt_data)


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
    df = grid_to_pandas(grid)

    assert df.iloc[0]["productName"] == "Acme Haystack Server"
    assert df.iloc[0]["tz"] == "New_York"
    assert df.iloc[0]["haystackVersion"] == "4.0"
    assert df.iloc[0]["vendorUri"] == Uri("http://acme.com/")
    assert df.iloc[0]["serverTime"] == server_time
