from datetime import date, datetime, timedelta
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pytest

from phable import (
    AuthError,
    DateRange,
    DateTimeRange,
    Grid,
    HaystackClient,
    Marker,
    Number,
    Ref,
    UnknownRecError,
    open_haystack_client,
)


# -----------------------------------------------------------------------------
# auth tests
# -----------------------------------------------------------------------------


def test_open(URI: str, USERNAME: str, PASSWORD: str):
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
    assert "s-" in auth_token


def test_open_client(URI: str, USERNAME: str, PASSWORD: str):
    with open_haystack_client(URI, USERNAME, PASSWORD) as hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "s-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

        auth_token = hc._auth_token

    with pytest.raises(HTTPError) as e:
        HaystackClient(URI, auth_token).about()

    assert e.value.status == 403


def test_close_op(URI: str, USERNAME: str, PASSWORD: str):
    client = HaystackClient.open(URI, USERNAME, PASSWORD)
    assert len(client.close().rows) == 0


# -----------------------------------------------------------------------------
# haystack op tests
# -----------------------------------------------------------------------------


def test_about_op(client: HaystackClient):
    assert client.about()["vendorName"] == "SkyFoundry"


def test_about_op_with_trailing_uri_slash(URI: str, USERNAME: str, PASSWORD: str):
    client = HaystackClient.open(URI + "/", USERNAME, PASSWORD)
    assert client.about()["vendorName"] == "SkyFoundry"
    client.close()


def test_about_op_with_trailing_uri_slash_using_context(
    URI: str, USERNAME: str, PASSWORD: str
):
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

    date_range = date.today()
    his_grid = client.his_read_by_ids(test_pt_rec["id"], date_range)

    expected_values = [
        (0, 72.2, "kW"),
        (1, 76.3, "kW"),
    ]
    for row_idx, expected_val, expected_unit in expected_values:
        assert his_grid.rows[row_idx]["val"].val == pytest.approx(expected_val)
        assert his_grid.rows[row_idx]["val"].unit == expected_unit


def test_batch_his_write_by_ids(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]],
    client: HaystackClient,
):
    test_rec_id_1, data_1 = point_id_with_his_data
    test_rec_id_2, data_2 = point_id_with_his_data

    date_range = date.today()
    his_grid = client.his_read_by_ids([test_rec_id_1, test_rec_id_2], date_range)

    for index in range(len(his_grid.rows)):
        assert his_grid.rows[index]["v0"].val == pytest.approx(data_1[index]["v0"].val)
        assert his_grid.rows[index]["v0"].unit == pytest.approx(
            data_1[index]["v0"].unit
        )
        assert his_grid.rows[index]["v1"].val == pytest.approx(data_2[index]["v0"].val)
        assert his_grid.rows[index]["v1"].unit == pytest.approx(
            data_2[index]["v0"].unit
        )


def test_his_read_by_id_with_date_range(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]],
    client: HaystackClient,
):
    test_rec_id, _ = point_id_with_his_data

    # get the his using Date as the range
    start = date.today()
    his_grid = client.his_read_by_id(test_rec_id, start)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_date_range(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]], client: HaystackClient
):
    # find the point ids
    point_ref1, _ = point_id_with_his_data
    point_ref2, _ = point_id_with_his_data

    # get the his using Date as the range
    start = date.today()
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


def test_his_read_by_ids_with_datetime_range(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]], client: HaystackClient
):
    # find the point ids
    point_ref, _ = point_id_with_his_data
    ts_now = datetime.now(ZoneInfo("America/New_York"))

    # get the his using Date as the range
    datetime_range = DateTimeRange(
        datetime(
            ts_now.year,
            ts_now.month,
            ts_now.day,
            0,
            0,
            0,
            tzinfo=ZoneInfo("America/New_York"),
        ),
        datetime(
            ts_now.year,
            ts_now.month,
            ts_now.day,
            23,
            59,
            59,
            tzinfo=ZoneInfo("America/New_York"),
        ),
    )
    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == datetime_range.start.date()
    assert his_grid.rows[-1][cols[0]].date() == date.today()


def test_his_read_by_ids_with_date_slice(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]], client: HaystackClient
):
    # find the point id
    point_ref, _ = point_id_with_his_data

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
    assert his_grid.meta["hisStart"].date() == start
    # hisEnd on a his read always goes to midnight of the end date which resolves as tomorrow in date()
    assert his_grid.meta["hisEnd"].date() == end + timedelta(days=1)


def test_his_read_by_ids_with_datetime_slice(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]], client: HaystackClient
):
    # find the point id
    point_ref, _ = point_id_with_his_data

    # get the his using Date as the range
    ts_now = datetime.now(ZoneInfo("America/New_York"))
    date_now = datetime(
        ts_now.year,
        ts_now.month,
        ts_now.day,
        0,
        0,
        0,
        tzinfo=ZoneInfo("America/New_York"),
    )
    start = date_now - timedelta(days=3)
    end = date_now + timedelta(hours=23, minutes=59, seconds=59)

    datetime_range = DateTimeRange(start, end)

    his_grid = client.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.meta["hisStart"].date() == start.date()
    assert his_grid.meta["hisEnd"].date() == end.date()


def test_batch_his_read_by_ids(
    point_id_with_his_data: tuple[Ref, list[dict[str, Any]]], client: HaystackClient
):
    ids = []
    for _ in range(4):
        new_id, _ = point_id_with_his_data
        ids.append(new_id)

    his_grid = client.his_read_by_ids(ids, date.today())

    cols = [col.name for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[0]], datetime)
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert isinstance(his_grid.rows[0][cols[4]], Number)
    assert his_grid.rows[0][cols[4]].unit == "kW"
    assert his_grid.rows[0][cols[4]].val >= 0


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
