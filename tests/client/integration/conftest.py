from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any, Callable, Generator, Mapping
from zoneinfo import ZoneInfo

import pytest

from phable import (
    CallError,
    HaxallClient,
    HaystackClient,
    open_haxall_client,
)
from phable.kinds import Marker, Number, Ref

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


@pytest.fixture(params=["json", "zinc"], scope="module")
def client(request) -> Generator[HaystackClient, None, None]:
    hc = HaxallClient.open(_URI, _USERNAME, _PASSWORD, content_type=request.param)

    yield hc

    hc.close()


@pytest.fixture(scope="session", autouse=True)
def configure_and_teardown_proj() -> Generator[None, None, None]:
    with open_haxall_client(_URI, _USERNAME, _PASSWORD, content_type="json") as client:
        try:
            _seed_pytest_recs(client)
        except BaseException:
            _clear_pytest_recs(client)
            raise

        yield

        _clear_pytest_recs(client)

        point_count = client.eval("readCount(point and pytest)").rows[0]["val"].val
        equip_count = client.eval("readCount(equip and pytest)").rows[0]["val"].val
        site_count = client.eval("readCount(site and pytest)").rows[0]["val"].val

        if point_count != 0:
            raise ValueError(
                f"Unexpected number of pytest points in database. {point_count} != 0"
            )
        if equip_count != 0:
            raise ValueError(
                f"Unexpected number of pytest equips in database. {equip_count} != 0"
            )
        if site_count != 0:
            raise ValueError(
                f"Unexpected number of pytest sites in database. {site_count} != 0"
            )


def _seed_pytest_recs(client: HaxallClient) -> None:
    data = [
        {
            "id": Ref("ph-001"),
            "site": Marker(),
            "pytest": Marker(),
            "dis": "Carytown",
            "geoState": "VA",
        },
        {
            "id": Ref("ph-002"),
            "siteRef": Ref("ph-001"),
            "equip": Marker(),
            "pytest": Marker(),
            "siteMeter": Marker(),
            "dis": "Elec-Meter-01",
            "elec": Marker(),
            "meter": Marker(),
        },
        {
            "id": Ref("ph-003"),
            "siteRef": Ref("ph-001"),
            "equipRef": Ref("ph-002"),
            "point": Marker(),
            "pytest": Marker(),
            "his": Marker(),
            "demand": Marker(),
            "navName": "kW",
            "kind": "Number",
            "unit": "kW",
            "tz": "New_York",
        },
        {
            "id": Ref("ph-004"),
            "siteRef": Ref("ph-001"),
            "equipRef": Ref("ph-002"),
            "point": Marker(),
            "pytest": Marker(),
            "his": Marker(),
            "demand": Marker(),
            "navName": "kW",
            "kind": "Number",
            "unit": "kW",
            "tz": "New_York",
        },
    ]

    try:
        client.commit_add(data)
    except CallError as e:
        if "Rec already exists" in e.help_msg.meta.get("errTrace", ""):
            print("Previous test records still in database, clearing then re-adding")
            _clear_pytest_recs(client)
            client.commit_add(data)
        else:
            raise

    try:
        client.eval('libAdd("hx.point")')
    except CallError as e:
        if "Lib already enabled: hx.point" not in e.help_msg.meta.get("errTrace", ""):
            raise

    point_count = client.eval("readCount(point and pytest)").rows[0]["val"].val
    equip_count = client.eval("readCount(equip and pytest)").rows[0]["val"].val
    site_count = client.eval("readCount(site and pytest)").rows[0]["val"].val

    if point_count != 2:
        raise ValueError(
            f"Unexpected number of pytest points in database. {point_count} != 2"
        )
    if equip_count != 1:
        raise ValueError(
            f"Unexpected number of pytest equips in database. {equip_count} != 1"
        )
    if site_count != 1:
        raise ValueError(
            f"Unexpected number of pytest sites in database. {site_count} != 1"
        )


def _clear_pytest_recs(client: HaxallClient) -> None:
    try:
        client.eval(
            "readAll((site or point or equip) and pytest).toRecList.map(r=> diff(r, null, {remove})).commit()"
        )
    except BaseException as e:
        print(f"Failed to clear pytest recs: {e}")


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
