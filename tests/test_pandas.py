from datetime import datetime, timedelta
from typing import Generator
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from phable.client import Client
from phable.kinds import NA, Grid, Number, Ref, Uri

phable_parsers_pandas = pytest.importorskip("phable.parsers.pandas")


URI = "http://localhost:8080/api/demo"
USERNAME = "su"
PASSWORD = "su"


@pytest.fixture(scope="module")
def client() -> Generator[Client, None, None]:
    hc = Client.open(URI, USERNAME, PASSWORD)

    yield hc

    hc.close()


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
    df = his_grid.to_pandas()

    assert df.attrs["meta"] == meta
    # assert df.attrs["cols"] == cols
    assert df.attrs["cols"] == [
        {"name": "ts"},
        {
            "name": "val",
            "meta": {
                "id": Ref("1234", "foo kW"),
                "kind": "Number",
                "unit": "kW",
            },
        },
    ]

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
    df = phable_parsers_pandas.grid_to_pandas(his_grid)

    assert df.attrs["meta"] == meta

    cols_with_new_meta = cols = [
        {"name": "ts"},
        {
            "name": "v0",
            "meta": {
                "id": Ref("1234", "foo1 kW"),
                "kind": "Number",
                "unit": "kW",
            },
        },
        {
            "name": "v1",
            "meta": {
                "id": Ref("2345", "foo2 W"),
                "kind": "Number",
                "unit": "W",
            },
        },
    ]

    assert df.attrs["cols"] == cols_with_new_meta

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
        phable_parsers_pandas.get_col_meta(df_attrs, "Headquarters ElecMeter-Main kW")
        == col_meta1
    )

    assert (
        phable_parsers_pandas.get_col_meta(
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

    with pytest.raises(phable_parsers_pandas.NotFoundError):
        phable_parsers_pandas.get_col_meta(df_attrs, "Hello World!")


def test_get_col_meta_raises_unit_mismatch_error(client: Client) -> None:
    meta = {"id": Ref("435", "Test")}
    rows = [
        {
            "ts": datetime.now() - timedelta(minutes=5),
            "val": Number(12, "kW"),
        },
        {
            "ts": datetime.now(),
            "val": Number(24, "W"),
        },
    ]
    cols = [{"name": "ts"}, {"name": "val"}]
    his_grid = Grid(meta, cols, rows)
    with pytest.raises(phable_parsers_pandas.UnitMismatchError):
        his_grid.to_pandas()


def test_single_col_his_grid_to_pandas() -> None:
    ts_now = datetime.now(ZoneInfo("America/New_York"))

    foo = Ref("1234", "foo kW")

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
    df = phable_parsers_pandas.grid_to_pandas(his_grid)

    assert df.loc[ts_now]["foo kW"] == 76.3
    assert df.loc[ts_now - timedelta(seconds=30)]["foo kW"] == 72.2
    assert {
        "name": "val",
        "meta": phable_parsers_pandas._get_col_meta_by_name(df.attrs["cols"], "val"),
    } == {
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
    df = phable_parsers_pandas.grid_to_pandas(his_grid)

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
    df = phable_parsers_pandas.grid_to_pandas(his_grid)

    assert df.loc[ts_now - timedelta(seconds=30)]["foo1 kW"] == 72.2
    assert df.loc[ts_now - timedelta(seconds=30)]["foo2 kW"] == 76.3

    assert df.loc[ts_now]["foo1 kW"] == 76.3
    assert df.loc[ts_now]["foo2 kW"] == 72.2

    assert {
        "name": "v0",
        "meta": phable_parsers_pandas._get_col_meta_by_name(df.attrs["cols"], "v0"),
    } == {
        "name": "v0",
        "meta": {
            "id": foo1,
            "kind": "Number",
            "unit": "kW",
        },
    }

    assert {
        "name": "v1",
        "meta": phable_parsers_pandas._get_col_meta_by_name(df.attrs["cols"], "v1"),
    } == {
        "name": "v1",
        "meta": {
            "id": foo2,
            "kind": "Number",
            "unit": "kW",
        },
    }

    assert isinstance(df.attrs["meta"], dict)
    assert df.attrs["meta"]["ver"] == "3.0"


def test_multi_col_his_grid_to_pandas_raises_duplicate_col_name_error() -> None:
    foo1 = Ref("1234", "foo1 kW")
    foo2 = Ref("2345", "foo1 kW")

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

    with pytest.raises(phable_parsers_pandas.DuplicateColNameError):
        phable_parsers_pandas.grid_to_pandas(his_grid)


def test_grid_to_pandas() -> None:
    server_time = datetime(2021, 5, 31, 11, 23, 23, tzinfo=ZoneInfo("America/New_York"))

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
    df = phable_parsers_pandas.grid_to_pandas(grid)

    assert df.iloc[0]["productName"] == "Acme Haystack Server"
    assert df.iloc[0]["tz"] == "New_York"
    assert df.iloc[0]["haystackVersion"] == "4.0"
    assert df.iloc[0]["vendorUri"] == Uri("http://acme.com/")
    assert df.iloc[0]["serverTime"] == server_time


def test_grid_with_na_to_pandas():
    ts_now = datetime.now(ZoneInfo("America/New_York"))

    foo = Ref("1234", "foo kW")

    meta = {"ver": "3.0", "id": foo}
    cols = [{"name": "ts"}, {"name": "val"}]
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "val": Number(0, "kW"),
        },
        {"ts": ts_now, "val": NA()},
    ]

    his_df = Grid(meta, cols, rows).to_pandas()

    assert pd.isna(his_df.iloc[1]["foo kW"])


def test_grid_with_diff_cols_in_rows():
    foo1 = Ref("1234", "foo1 kW")
    foo2 = Ref("2345", "foo2 kW")

    ts_now = datetime.now(ZoneInfo("America/New_York"))
    meta = {"ver": "3.0"}
    cols = [
        {"name": "ts"},
        {"name": "v0", "meta": {"id": foo1}},
        {"name": "v1", "meta": {"id": foo2}},
    ]
    rows = [
        {"ts": ts_now - timedelta(seconds=30), "v0": Number(72.2, "kW")},
        {"ts": ts_now, "v1": Number(72.2, "kW")},
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2, "kW"),
            "v1": Number(76.3, "kW"),
        },
    ]

    his_df = Grid(meta, cols, rows).to_pandas()

    assert pd.isna(his_df.iloc[0]["foo2 kW"])
    assert pd.isna(his_df.iloc[1]["foo1 kW"])
