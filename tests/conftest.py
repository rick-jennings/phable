from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any, Callable, Generator, Mapping
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from phable import (
    HaxallClient,
    HaystackClient,
)
from phable.kinds import NA, Grid, GridCol, Marker, Number, Ref

_URI = "http://localhost:8080/api/sys"
_USERNAME = "su"
_PASSWORD = "su"


@pytest.fixture
def URI() -> str:
    return _URI


@pytest.fixture
def USERNAME() -> str:
    return _USERNAME


@pytest.fixture
def PASSWORD() -> str:
    return _PASSWORD


TS_NOW = datetime.now(ZoneInfo("America/New_York"))

EXPECTED_SCHEMA = pa.schema(
    [
        ("id", pa.dictionary(pa.int32(), pa.string())),
        ("ts", pa.timestamp("us", tz="America/New_York")),
        ("val_bool", pa.bool_()),
        ("val_str", pa.string()),
        ("val_num", pa.float64()),
        ("val_na", pa.bool_()),
    ]
)


@pytest.fixture(params=["json", "zinc"], scope="module")
def client(request) -> Generator[HaystackClient, None, None]:
    hc = HaxallClient.open(_URI, _USERNAME, _PASSWORD, content_type=request.param)

    yield hc

    hc.close()


@pytest.fixture(scope="module")
def non_his_grid() -> Grid:
    meta = {"ver": "3.0"}
    cols = [
        GridCol("x"),
        GridCol("y"),
    ]
    rows = [{"x": 1}, {"y": 2}]

    return Grid(meta, cols, rows)


@pytest.fixture(scope="module")
def single_pt_his_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [
        GridCol("ts"),
        GridCol(
            "val",
            {
                "id": Ref("point1", "Point 1 description"),
                "unit": "kW",
                "kind": "Number",
            },
        ),
    ]
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


@pytest.fixture(scope="module")
def single_pt_his_table() -> pa.Table:
    data = [
        {
            "id": "point1",
            "ts": TS_NOW - timedelta(seconds=60),
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "val_na": True,
        },
        {
            "id": "point1",
            "ts": TS_NOW - timedelta(seconds=30),
            "val_bool": None,
            "val_str": None,
            "val_num": 72.2,
            "val_na": None,
        },
        {
            "id": "point1",
            "ts": TS_NOW,
            "val_bool": None,
            "val_str": None,
            "val_num": 76.3,
            "val_na": None,
        },
    ]

    return pa.Table.from_pylist(data, schema=EXPECTED_SCHEMA)


@pytest.fixture(scope="module")
def multi_pt_his_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [
        GridCol("ts"),
        GridCol("v0", {"id": Ref("point1", "Power"), "unit": "kW", "kind": "Number"}),
        GridCol("v1", {"id": Ref("point2", "Status"), "kind": "Str"}),
        GridCol("v2", {"id": Ref("point3"), "kind": "Bool"}),
    ]
    rows = [
        {
            "ts": TS_NOW - timedelta(seconds=60),
            # v0 is None (missing from row)
            "v1": "available",
            "v2": True,
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "v0": NA(),
            # v1 is None (missing from row)
            "v2": False,
        },
        {
            "ts": TS_NOW,
            "v0": Number(76.3, "kW"),
            "v1": NA(),
            # v2 is None (missing from row)
        },
    ]

    return Grid(meta, cols, rows)


@pytest.fixture(scope="module")
def multi_pt_his_table() -> pa.Table:
    data = [
        {
            "id": "point1",
            "ts": TS_NOW - timedelta(seconds=30),
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "val_na": True,
        },
        {
            "id": "point1",
            "ts": TS_NOW,
            "val_bool": None,
            "val_str": None,
            "val_num": 76.3,
            "val_na": None,
        },
        {
            "id": "point2",
            "ts": TS_NOW - timedelta(seconds=60),
            "val_bool": None,
            "val_str": "available",
            "val_num": None,
            "val_na": None,
        },
        {
            "id": "point2",
            "ts": TS_NOW,
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "val_na": True,
        },
        {
            "id": "point3",
            "ts": TS_NOW - timedelta(seconds=60),
            "val_bool": True,
            "val_str": None,
            "val_num": None,
            "val_na": None,
        },
        {
            "id": "point3",
            "ts": TS_NOW - timedelta(seconds=30),
            "val_bool": False,
            "val_str": None,
            "val_num": None,
            "val_na": None,
        },
    ]

    return pa.Table.from_pylist(data, schema=EXPECTED_SCHEMA)




@pytest.fixture(scope="module")
def create_kw_pt_rec_fn(
    client: HaxallClient,
) -> Generator[Callable[[], dict[str, Any]], None, None]:
    axon_expr = (
        """diff(null, {hisTest, pytest, point, his, tz: "New_York", writable, """
        """kind: "Number", unit: "kW"}, {add}).commit"""
    )
    created_pt_ids: list[Ref] = []

    def _create_pt_rec() -> Mapping[str, Any]:
        response = client.eval(axon_expr)
        pt_rec = response.rows[0]
        created_pt_ids.append(pt_rec["id"])
        return pt_rec

    yield _create_pt_rec

    for pt_id in created_pt_ids:
        axon_expr = f"readById(@{pt_id.val}).diff({{trash}}).commit"
        client.eval(axon_expr)


@pytest.fixture(scope="module")
def point_id_with_his_data(
    client: HaxallClient, create_kw_pt_rec_fn: Callable[[], dict[str, Any]]
) -> Generator[tuple[Ref, list[dict[str, Any]]], None, None]:
    test_pt_rec = create_kw_pt_rec_fn()

    ts_now = datetime.now(ZoneInfo("America/New_York"))

    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(random.randint(70, 80), "kW"),
        },
        {
            "ts": ts_now,
            "v0": Number(random.randint(70, 80), "kW"),
        },
    ]

    client.his_write_by_ids([test_pt_rec["id"]], rows)

    yield (test_pt_rec["id"], rows)


@pytest.fixture
def sample_recs() -> list[dict[str, Any]]:
    data = [
        {"dis": "Rec1...", "testing": Marker()},
        {"dis": "Rec2...", "testing": Marker()},
    ]
    return data


@pytest.fixture(scope="module")
def create_pt_that_is_not_removed_fn(
    client: HaxallClient,
) -> Generator[Callable[[], dict[str, Any]], None, None]:
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number"}, {add}).commit"""
    )

    def _create_pt():
        response = client.eval(axon_expr)
        writable_kw_pt_rec = response.rows[0]
        return writable_kw_pt_rec

    yield _create_pt
