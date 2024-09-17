from datetime import date, datetime, timedelta
from typing import Any, Callable, Generator
from urllib.error import URLError
from zoneinfo import ZoneInfo

import pytest

from phable import (
    AuthError,
    Client,
    DateRange,
    DateTimeRange,
    Grid,
    HxClient,
    Marker,
    Number,
    Ref,
    UnknownRecError,
    open_client,
)
from phable.http import IncorrectHttpResponseStatus

# Note:  These tests are made using SkySpark as the Haystack server
URI = "http://localhost:8080/api/demo"
USERNAME = "su"
PASSWORD = "su"


@pytest.fixture(scope="module")
def client() -> Generator[Client, None, None]:
    # use HxClient's features to test Client
    hc = HxClient.open(URI, USERNAME, PASSWORD)

    yield hc

    hc.close()


@pytest.fixture(scope="module")
def create_kw_pt_rec_fn(
    client: Client,
) -> Generator[Callable[[], dict[str, Any]], None, None]:
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number", unit: "kW"}, {add}).commit"""
    )
    created_pt_ids = []

    def _create_pt_rec():
        response = client.eval(axon_expr)
        pt_rec = response.rows[0]
        created_pt_ids.append(pt_rec["id"])
        return pt_rec

    yield _create_pt_rec

    for pt_id in created_pt_ids:
        axon_expr = f"readById(@{pt_id}).diff({{trash}}).commit"
        client.eval(axon_expr)


# -----------------------------------------------------------------------------
# auth tests
# -----------------------------------------------------------------------------


def test_open():
    with pytest.raises(AuthError):
        Client.open(URI, USERNAME, "wrong_password")

    with pytest.raises(AuthError):
        Client.open(URI, "wrong_username", PASSWORD)

    with pytest.raises(URLError):
        Client.open("wrong-url", USERNAME, PASSWORD)

    with pytest.raises(TypeError):
        Client(URI, USERNAME, "wrong_password")


def test_auth_token(client: Client):
    auth_token = client._auth_token

    assert len(auth_token) > 40
    assert "web-" in auth_token


def test_open_client():
    with open_client(URI, USERNAME, PASSWORD) as hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "web-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

        auth_token = hc._auth_token

    with pytest.raises(IncorrectHttpResponseStatus) as incorrectHttpResponseStatus:
        Client._create(URI, auth_token).about()

    assert incorrectHttpResponseStatus.value.actual_status == 403


def test_close_op():
    client = Client.open(URI, USERNAME, PASSWORD)
    assert len(client.close().rows) == 0


# -----------------------------------------------------------------------------
# haystack op tests
# -----------------------------------------------------------------------------


def test_about_op(client: Client):
    assert client.about()["vendorName"] == "SkyFoundry"


def test_read_site(client: Client):
    grid = client.read('site and dis=="Carytown"')
    assert grid.rows[0]["geoState"] == "VA"


def test_read_UnknownRecError(client: Client):
    with pytest.raises(UnknownRecError):
        client.read("hi")


def test_read_no_error_when_checked_is_false(client: Client):
    assert len(client.read("hi", False).rows) == 0


def test_read_point(client: Client):
    grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    assert isinstance(grid.rows[0]["power"], Marker)


def test_read_by_id(client: Client):
    id1 = client.read("point and power and equipRef->siteMeter").rows[0]["id"]
    response = client.read_by_id(id1)

    assert response.rows[0]["navName"] == "kW"

    with pytest.raises(UnknownRecError):
        client.read_by_id(Ref("invalid-id"))

    checked_response = client.read_by_id(Ref("invalid-id"), False)
    assert len(checked_response.rows) == 0


def test_read_by_ids(client: Client):
    ids = client.read_all("point and power and equipRef->siteMeter")
    id1 = ids.rows[0]["id"]
    id2 = ids.rows[1]["id"]

    response = client.read_by_ids([id1, id2])

    assert response.rows[0]["tz"] == "New_York"
    assert response.rows[1]["tz"] == "New_York"

    with pytest.raises(UnknownRecError):
        client.read_by_ids([id1, Ref("invalid-id")])

    with pytest.raises(UnknownRecError):
        client.read_by_ids([Ref("invalid-id"), id2])

    with pytest.raises(UnknownRecError):
        client.read_by_ids([Ref("invalid-id1"), Ref("invalid-id2")])


def test_his_read_by_id_with_date_range(client: Client):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    his_grid = client.his_read_by_id(point_ref, start)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_date_range(client: Client):
    # find the point ids
    point_grid = client.read_all("""point and equipRef->siteMeter and power""")
    point_ref1 = point_grid.rows[0]["id"]
    point_ref2 = point_grid.rows[1]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    his_grid = client.his_read_by_ids([point_ref1, point_ref2], start)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[1][cols[1]].unit == "kW"
    assert his_grid.rows[1][cols[1]].val >= 0
    assert his_grid.rows[1][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_datetime_range(client: Client):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    datetime_range = DateTimeRange(
        datetime(2023, 8, 20, 10, 12, 12, tzinfo=ZoneInfo("America/New_York"))
    )
    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == datetime_range.start.date()
    assert his_grid.rows[-1][cols[0]].date() == date.today()


def test_his_read_by_ids_with_date_slice(client: Client):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    end = date.today()
    date_range = DateRange(start, end)
    his_grid = client.his_read_by_ids(point_ref, date_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == end


def test_his_read_by_ids_with_datetime_slice(client: Client):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = datetime(2023, 8, 20, 12, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    end = start + timedelta(days=3)

    datetime_range = DateTimeRange(start, end)

    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start.date()
    assert his_grid.rows[-1][cols[0]].date() == end.date()


def test_batch_his_read_by_ids(client: Client):
    ids = client.read_all("point and power and equipRef->siteMeter")
    id1 = ids.rows[0]["id"]
    id2 = ids.rows[1]["id"]
    id3 = ids.rows[2]["id"]
    id4 = ids.rows[3]["id"]

    ids = [id1, id2, id3, id4]
    his_grid = client.his_read_by_ids(ids, date.today())

    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[0]], datetime)
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert isinstance(his_grid.rows[0][cols[4]], Number)
    assert his_grid.rows[0][cols[4]].unit == "kW"
    assert his_grid.rows[0][cols[4]].val >= 0


def test_his_read(client: Client):
    pt_grid = client.read("power and point and equipRef->siteMeter")
    his_grid = client.his_read(pt_grid, date.today())

    his_grid_cols = his_grid.cols

    assert his_grid_cols[0]["name"] == "ts"
    assert his_grid_cols[1]["meta"]["power"] == Marker()
    assert his_grid_cols[1]["meta"]["point"] == Marker()
    assert his_grid_cols[-1]["meta"]["power"] == Marker()
    assert his_grid_cols[-1]["meta"]["point"] == Marker()

    assert isinstance(his_grid.rows[0]["ts"], datetime)
    assert isinstance(his_grid.rows[0]["v0"], Number)
    assert his_grid.rows[0]["v0"].unit == "kW"
    assert his_grid.rows[0]["v0"].val >= 0


def test_single_his_write_by_id(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    test_pt_rec = create_kw_pt_rec_fn()

    ts_now = datetime.now(ZoneInfo("America/New_York"))
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

    # write the his data to the test pt
    response = client.his_write_by_id(test_pt_rec["id"], rows)

    assert len(response.rows) == 0

    range = date.today()
    his_grid = client.his_read_by_ids(test_pt_rec["id"], range)

    assert his_grid.rows[0]["val"] == Number(pytest.approx(72.2), "kW")
    assert his_grid.rows[1]["val"] == Number(pytest.approx(76.3), "kW")


def test_batch_his_write_by_ids(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    test_pt_rec1 = create_kw_pt_rec_fn()
    test_pt_rec2 = create_kw_pt_rec_fn()

    ts_now = datetime.now(ZoneInfo("America/New_York"))

    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2, "kW"),
            "v1": Number(76.3, "kW"),
        },
        {"ts": ts_now, "v0": Number(76.3), "v1": Number(72.2)},
    ]

    # write the his data to the test pt
    response = client.his_write_by_ids([test_pt_rec1["id"], test_pt_rec2["id"]], rows)
    assert len(response.rows) == 0

    range = date.today()
    his_grid = client.his_read_by_ids([test_pt_rec1["id"], test_pt_rec2["id"]], range)

    assert his_grid.rows[0]["v0"] == Number(pytest.approx(72.2), "kW")
    assert his_grid.rows[1]["v0"] == Number(pytest.approx(76.3), "kW")
    assert his_grid.rows[0]["v1"] == Number(pytest.approx(76.3), "kW")
    assert his_grid.rows[1]["v1"] == Number(pytest.approx(72.2), "kW")


def test_client_his_read_with_pandas(client: Client):
    # We are importing pandas here only to check that it can be imported.
    # This can be improved in the future.
    pytest.importorskip("pandas")
    pts = client.read_all("point and power and equipRef->siteMeter")
    pts_his_df = client.his_read(pts, date.today()).to_pandas()

    for col in pts_his_df.attrs["cols"]:
        if col["name"] == "ts":
            continue
        assert col["meta"]["power"] == Marker()
        assert col["meta"]["point"] == Marker()
        assert col["meta"]["kind"] == "Number"
        assert col["meta"]["unit"] == "kW"

    assert pts_his_df.attrs["cols"][0]["name"] == "ts"
    assert pts_his_df.attrs["cols"][1]["name"] == "v0"
    assert pts_his_df.attrs["cols"][2]["name"] == "v1"
    assert pts_his_df.attrs["cols"][3]["name"] == "v2"
    assert pts_his_df.attrs["cols"][4]["name"] == "v3"
    assert len(pts_his_df.attrs["cols"]) == 5
    assert "ver" in pts_his_df.attrs["meta"].keys()
    assert "hisStart" in pts_his_df.attrs["meta"].keys()
    assert "hisEnd" in pts_his_df.attrs["meta"].keys()


def test_client_his_read_by_ids_with_pandas(client: Client):
    # We are importing pandas here only to check that it can be imported.
    # This can be improved in the future.
    pytest.importorskip("pandas")
    pts = client.read_all("point and power and equipRef->siteMeter")
    pts_his_df = client.his_read_by_ids(
        [pt_row["id"] for pt_row in pts.rows], date.today()
    ).to_pandas()

    for col in pts_his_df.attrs["cols"]:
        if col["name"] == "ts":
            continue
        assert col["meta"]["kind"] == "Number"
        assert col["meta"]["unit"] == "kW"
        assert len(col["meta"]) == 3

    assert pts_his_df.attrs["cols"][0]["name"] == "ts"
    assert pts_his_df.attrs["cols"][1]["name"] == "v0"
    assert pts_his_df.attrs["cols"][2]["name"] == "v1"
    assert pts_his_df.attrs["cols"][3]["name"] == "v2"
    assert pts_his_df.attrs["cols"][4]["name"] == "v3"
    assert len(pts_his_df.attrs["cols"]) == 5
    assert "ver" in pts_his_df.attrs["meta"].keys()
    assert "hisStart" in pts_his_df.attrs["meta"].keys()
    assert "hisEnd" in pts_his_df.attrs["meta"].keys()


def test_point_write_number(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1, Number(0, "kW"))

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_number_who(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1, Number(50, "kW"), "Phable")

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []

    check_response = client.point_write_array(pt_rec["id"])
    check_row = check_response.rows[0]

    assert check_row["val"] == Number(50, "kW")
    assert "Phable" in check_row["who"]
    assert "expires" not in check_row.keys()


def test_point_write_number_who_dur(
    create_kw_pt_rec_fn: Callable[[], Ref], client: Client
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(
        pt_rec["id"], 8, Number(100, "kW"), "Phable", Number(5, "min")
    )

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []

    check_response = client.point_write_array(pt_rec["id"])
    check_row = check_response.rows[7]
    expires = check_row["expires"]

    assert check_row["val"] == Number(100, "kW")
    assert "Phable" in check_row["who"]
    assert expires.unit == "min"
    assert expires.val > 4.0 and expires.val < 5.0


def test_point_write_null(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1)

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_array(create_kw_pt_rec_fn: Callable[[], Ref], client: Client):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write_array(pt_rec["id"])

    assert response.rows[0]["level"] == Number(1)
    assert response.rows[-1]["level"] == Number(17)
