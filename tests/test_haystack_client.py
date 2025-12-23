from datetime import date, datetime, timedelta
from typing import Any, Callable, Generator
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pytest

from phable import (
    AuthError,
    DateRange,
    DateTimeRange,
    Grid,
    HaxallClient,
    HaystackClient,
    Marker,
    Number,
    Ref,
    UnknownRecError,
    open_haystack_client,
)

# Note:  These tests are made using SkySpark as the Haystack server
URI = "http://localhost:8080/api/demo"
USERNAME = "su"
PASSWORD = "su"


@pytest.fixture(params=["json", "zinc"], scope="module")
def client(request) -> Generator[HaystackClient, None, None]:
    # use HxClient's features to test Client
    hc = HaxallClient.open(URI, USERNAME, PASSWORD, content_type=request.param)

    yield hc

    hc.close()


@pytest.fixture(scope="module")
def create_kw_pt_rec_fn(
    client: HaxallClient,
) -> Generator[Callable[[], dict[str, Any]], None, None]:
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number", unit: "kW"}, {add}).commit"""
    )
    created_pt_ids = []

    def _create_pt_rec() -> dict[str, Any]:
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
        HaystackClient.open(URI, USERNAME, "wrong_password")

    with pytest.raises(AuthError):
        HaystackClient.open(URI, "wrong_username", PASSWORD)

    with pytest.raises(URLError):
        HaystackClient.open("wrong-url1", USERNAME, PASSWORD)

    with pytest.raises(URLError):
        x = HaystackClient.open("http://wrong-url2", USERNAME, PASSWORD)
        x.about()

    with pytest.raises(TypeError):
        HaystackClient(URI, USERNAME, "wrong_password")  # ty: ignore[too-many-positional-arguments]


def test_auth_token(client: HaystackClient):
    auth_token = client._auth_token

    assert len(auth_token) > 40
    assert "web-" in auth_token


def test_open_client():
    with open_haystack_client(URI, USERNAME, PASSWORD) as hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "web-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

        auth_token = hc._auth_token

    with pytest.raises(HTTPError) as e:
        HaystackClient(URI, auth_token).about()

    assert e.value.status == 403


def test_close_op():
    client = HaystackClient.open(URI, USERNAME, PASSWORD)
    assert len(client.close().rows) == 0


# -----------------------------------------------------------------------------
# haystack op tests
# -----------------------------------------------------------------------------


def test_about_op(client: HaystackClient):
    assert client.about()["vendorName"] == "SkyFoundry"


def test_about_op_with_trailing_uri_slash():
    client = HaystackClient.open(URI + "/", USERNAME, PASSWORD)
    assert client.about()["vendorName"] == "SkyFoundry"
    client.close()


def test_about_op_with_trailing_uri_slash_using_context():
    with open_haystack_client(URI + "/", USERNAME, PASSWORD) as client:
        assert client.about()["vendorName"] == "SkyFoundry"


def test_read_site(client: HaystackClient):
    grid = client.read('site and dis=="Carytown"')
    assert grid["geoState"] == "VA"


def test_read_UnknownRecError(client: HaystackClient):
    with pytest.raises(UnknownRecError):
        client.read("hi")


def test_read_no_error_when_checked_is_false(client: HaystackClient):
    assert len(client.read("hi", False)) == 0


def test_read_point(client: HaystackClient):
    grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and demand"""
    )
    assert isinstance(grid["demand"], Marker)


def test_read_by_id(client: HaystackClient):
    id1 = client.read("point and demand and equipRef->siteMeter")["id"]
    response = client.read_by_id(id1)

    assert response["navName"] == "kW"

    with pytest.raises(UnknownRecError):
        client.read_by_id(Ref("invalid-id"))

    checked_response = client.read_by_id(Ref("invalid-id"), False)
    assert len(checked_response) == 0


def test_read_by_ids(client: HaystackClient):
    ids = client.read_all("point and demand and equipRef->siteMeter")
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


def test_his_read_by_id_with_date_range(client: HaystackClient):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and demand"""
    )
    point_ref = point_grid["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    his_grid = client.his_read_by_id(point_ref, start)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_date_range(client: HaystackClient):
    # find the point ids
    point_grid = client.read_all("""point and equipRef->siteMeter and demand""")
    point_ref1 = point_grid.rows[0]["id"]
    point_ref2 = point_grid.rows[1]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    his_grid = client.his_read_by_ids([point_ref1, point_ref2], start)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[1][cols[1]].unit == "kW"
    assert his_grid.rows[1][cols[1]].val >= 0
    assert his_grid.rows[1][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_datetime_range(client: HaystackClient):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and demand"""
    )
    point_ref = point_grid["id"]

    # get the his using Date as the range
    datetime_range = DateTimeRange(
        datetime(2024, 8, 20, 10, 12, 12, tzinfo=ZoneInfo("America/New_York"))
    )
    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == datetime_range.start.date()
    assert his_grid.rows[-1][cols[0]].date() == date.today()


def test_his_read_by_ids_with_date_slice(client: HaystackClient):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and demand"""
    )
    point_ref = point_grid["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    end = date.today()
    date_range = DateRange(start, end)
    his_grid = client.his_read_by_ids(point_ref, date_range)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == end


def test_his_read_by_ids_with_datetime_slice(client: HaystackClient):
    # find the point id
    point_grid = client.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and demand"""
    )
    point_ref = point_grid["id"]

    # get the his using Date as the range
    start = datetime(2024, 8, 20, 12, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    end = start + timedelta(days=3)

    datetime_range = DateTimeRange(start, end)

    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start.date()
    assert his_grid.rows[-1][cols[0]].date() == end.date()


def test_batch_his_read_by_ids(client: HaystackClient):
    ids = client.read_all("point and demand and equipRef->siteMeter")
    id1 = ids.rows[0]["id"]
    id2 = ids.rows[1]["id"]
    id3 = ids.rows[2]["id"]
    id4 = ids.rows[3]["id"]

    ids = [id1, id2, id3, id4]
    his_grid = client.his_read_by_ids(ids, date.today())

    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[0]], datetime)
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert isinstance(his_grid.rows[0][cols[4]], Number)
    assert his_grid.rows[0][cols[4]].unit == "kW"
    assert his_grid.rows[0][cols[4]].val >= 0


def test_single_his_write_by_id(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
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

    expected_values = [
        (0, 72.2, "kW"),
        (1, 76.3, "kW"),
    ]
    for row_idx, expected_val, expected_unit in expected_values:
        assert his_grid.rows[row_idx]["val"].val == pytest.approx(expected_val)
        assert his_grid.rows[row_idx]["val"].unit == expected_unit


def test_batch_his_write_by_ids(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
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

    expected_values = [
        (0, "v0", 72.2, "kW"),
        (1, "v0", 76.3, "kW"),
        (0, "v1", 76.3, "kW"),
        (1, "v1", 72.2, "kW"),
    ]
    for row_idx, col, expected_val, expected_unit in expected_values:
        assert his_grid.rows[row_idx][col].val == pytest.approx(expected_val)
        assert his_grid.rows[row_idx][col].unit == expected_unit


def test_point_write_number(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1, Number(0, "kW"))

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0].name == "empty"
    assert response.rows == []


def test_point_write_number_who(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1, Number(50, "kW"), "Phable")

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0].name == "empty"
    assert response.rows == []

    check_response = client.point_write_array(pt_rec["id"])
    check_row = check_response.rows[0]

    assert check_row["val"] == Number(50, "kW")
    assert "Phable" in check_row["who"]
    assert "expires" not in check_row.keys()


def test_point_write_number_who_dur(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(
        pt_rec["id"], 8, Number(100, "kW"), "Phable", Number(5, "min")
    )

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0].name == "empty"
    assert response.rows == []

    check_response = client.point_write_array(pt_rec["id"])
    check_row = check_response.rows[7]
    expires = check_row["expires"]

    assert check_row["val"] == Number(100, "kW")
    assert "Phable" in check_row["who"]
    assert expires.unit == "min"
    assert expires.val > 4.0 and expires.val < 5.0


def test_point_write_null(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write(pt_rec["id"], 1)

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0].name == "empty"
    assert response.rows == []


def test_point_write_array(
    create_kw_pt_rec_fn: Callable[[], dict[str, Any]], client: HaystackClient
):
    pt_rec = create_kw_pt_rec_fn()
    response = client.point_write_array(pt_rec["id"])

    assert response.rows[0]["level"] == Number(1)
    assert response.rows[-1]["level"] == Number(17)
