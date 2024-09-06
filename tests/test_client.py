from datetime import date, datetime, timedelta
from typing import Callable, Generator
from zoneinfo import ZoneInfo

import pytest

from phable import (
    Client,
    CommitFlag,
    DateRange,
    DateTimeRange,
    Grid,
    HaystackErrorGridResponseError,
    HaystackHisWriteOpParametersError,
    HaystackReadOpUnknownRecError,
    Marker,
    Number,
    Ref,
)
from phable.http import IncorrectHttpResponseStatus

# Note:  These tests are made using SkySpark as the Haystack server
URI = "http://localhost:8080/api/demo"
USERNAME = "su"
PASSWORD = "su"


@pytest.fixture(scope="module")
def hc() -> Generator[Client, None, None]:
    hc = Client(URI, USERNAME, PASSWORD)
    hc.open()

    yield hc

    hc.close()


@pytest.fixture(scope="module")
def create_kw_pt_fn(hc: Client) -> Generator[Callable[[], Ref], None, None]:
    axon_expr = (
        """diff(null, {pytest, point, his, tz: "New_York", writable, """
        """kind: "Number"}, {add}).commit"""
    )
    created_pt_ids = []

    def _create_pt():
        response = hc.eval(axon_expr)
        writable_kw_pt_id = response.rows[0]["id"]
        created_pt_ids.append(writable_kw_pt_id)
        return writable_kw_pt_id

    yield _create_pt

    for pt_id in created_pt_ids:
        axon_expr = f"readById(@{pt_id}).diff({{trash}}).commit"
        hc.eval(axon_expr)


# -----------------------------------------------------------------------------
# auth tests
# -----------------------------------------------------------------------------


def test_open():
    hc = Client(URI, USERNAME, "wrong_password")
    with pytest.raises(Exception):
        hc.open()


def test_auth_token(hc: Client):
    auth_token = hc._auth_token

    assert len(auth_token) > 40
    assert "web-" in auth_token


def test_context_manager():
    hc = Client(URI, USERNAME, PASSWORD)
    with hc:
        auth_token = hc._auth_token

        assert len(auth_token) > 40
        assert "web-" in auth_token
        assert hc.about()["vendorName"] == "SkyFoundry"

    with pytest.raises(IncorrectHttpResponseStatus) as incorrectHttpResponseStatus:
        hc.about()

    assert incorrectHttpResponseStatus.value.actual_status == 403


# -----------------------------------------------------------------------------
# haystack op tests
# -----------------------------------------------------------------------------


def test_about_op(hc: Client):
    assert hc.about()["vendorName"] == "SkyFoundry"


def test_read_site(hc: Client):
    grid = hc.read('site and dis=="Carytown"')
    assert grid.rows[0]["geoState"] == "VA"


def test_read_UnknownRecError(hc: Client):
    with pytest.raises(HaystackReadOpUnknownRecError):
        hc.read("hi")


def test_read_point(hc: Client):
    grid = hc.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    assert isinstance(grid.rows[0]["power"], Marker)


def test_read_by_id(hc: Client):
    id1 = hc.read("point and power and equipRef->siteMeter").rows[0]["id"]
    response = hc.read_by_ids(id1)

    assert response.rows[0]["navName"] == "kW"
    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(Ref("invalid-id"))


def test_read_by_ids(hc: Client):
    ids = hc.read("point and power and equipRef->siteMeter")
    id1 = ids.rows[0]["id"]
    id2 = ids.rows[1]["id"]

    response = hc.read_by_ids([id1, id2])

    assert response.rows[0]["tz"] == "New_York"
    assert response.rows[1]["tz"] == "New_York"

    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(
            [Ref("p:demo:r:2c26ff0c-d04a5b02"), Ref("invalid-id")]
        )

    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(
            [Ref("invalid-id"), Ref("p:demo:r:2c26ff0c-0b8c49a1")]
        )

    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids([Ref("invalid-id1"), Ref("invalid-id2")])


def test_his_read_by_ids_with_date_range(hc: Client):
    # find the point id
    point_grid = hc.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    his_grid = hc.his_read_by_ids(point_ref, start)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == start


def test_his_read_by_ids_with_datetime_range(hc: Client):
    # find the point id
    point_grid = hc.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    datetime_range = DateTimeRange(
        datetime(2023, 8, 20, 10, 12, 12, tzinfo=ZoneInfo("America/New_York"))
    )
    his_grid = hc.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == datetime_range.start.date()
    assert his_grid.rows[-1][cols[0]].date() == date.today()


def test_his_read_by_ids_with_date_slice(hc: Client):
    # find the point id
    point_grid = hc.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = date.today() - timedelta(days=7)
    end = date.today()
    date_range = DateRange(start, end)
    his_grid = hc.his_read_by_ids(point_ref, date_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start
    assert his_grid.rows[-1][cols[0]].date() == end


def test_his_read_by_ids_with_datetime_slice(hc: Client):
    # find the point id
    point_grid = hc.read(
        """point and siteRef->dis=="Carytown" and """
        """equipRef->siteMeter and power"""
    )
    point_ref = point_grid.rows[0]["id"]

    # get the his using Date as the range
    start = datetime(2023, 8, 20, 12, 12, 23, tzinfo=ZoneInfo("America/New_York"))
    end = start + timedelta(days=3)

    datetime_range = DateTimeRange(start, end)

    his_grid = hc.his_read_by_ids(point_ref, datetime_range)

    # check his_grid
    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert his_grid.rows[0][cols[0]].date() == start.date()
    assert his_grid.rows[-1][cols[0]].date() == end.date()


def test_batch_his_read_by_ids(hc: Client):
    ids = hc.read("point and power and equipRef->siteMeter")
    id1 = ids.rows[0]["id"]
    id2 = ids.rows[1]["id"]
    id3 = ids.rows[2]["id"]
    id4 = ids.rows[3]["id"]

    ids = [id1, id2, id3, id4]
    his_grid = hc.his_read_by_ids(ids, date.today())

    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[0]], datetime)
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert isinstance(his_grid.rows[0][cols[4]], Number)
    assert his_grid.rows[0][cols[4]].unit == "kW"
    assert his_grid.rows[0][cols[4]].val >= 0


def test_his_read(hc: Client):
    pt_grid = hc.read("power and point and equipRef->siteMeter")
    his_grid = hc.his_read(pt_grid, date.today())

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


def test_single_his_write_by_ids(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    test_pt_id = create_kw_pt_fn()

    ts_now = datetime.now(ZoneInfo("America/New_York"))
    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "val": Number(72.2),
        },
        {
            "ts": ts_now,
            "val": Number(76.3),
        },
    ]

    # write the his data to the test pt
    hc.his_write_by_ids(test_pt_id, rows)

    range = date.today()
    his_grid = hc.his_read_by_ids(test_pt_id, range)

    assert his_grid.rows[0]["val"] == Number(72.19999694824219)
    assert his_grid.rows[1]["val"] == Number(76.30000305175781)


def test_single_his_write_by_ids_wrong_his_rows(hc: Client):
    dt1 = datetime.now()
    his_rows1 = [
        {"ts": dt1 - timedelta(minutes=5), "val1": Number(1)},
        {"ts": dt1, "val1": Number(1)},
    ]

    with pytest.raises(HaystackHisWriteOpParametersError):
        hc.his_write_by_ids(Ref("abc"), his_rows1)

    dt2 = datetime.now()
    his_rows2 = [
        {"ts": dt2 - timedelta(minutes=5), "v0": Number(1)},
        {"ts": dt2, "v1": Number(1)},
    ]

    with pytest.raises(HaystackHisWriteOpParametersError):
        hc.his_write_by_ids(Ref("abc"), his_rows2)


def test_batch_his_write_by_ids_wrong_his_rows(hc: Client):
    dt1 = datetime.now()
    his_rows1 = [
        {"ts": dt1 - timedelta(minutes=5), "val": Number(1)},
        {"ts": dt1, "val": Number(1)},
    ]

    with pytest.raises(HaystackHisWriteOpParametersError):
        hc.his_write_by_ids([Ref("abc"), Ref("def")], his_rows1)

    dt2 = datetime.now()
    his_rows2 = [
        {"ts": dt2 - timedelta(minutes=5), "v0": Number(1)},
        {"ts": dt2, "v2": Number(1)},
    ]

    with pytest.raises(HaystackHisWriteOpParametersError):
        hc.his_write_by_ids([Ref("abc"), Ref("def")], his_rows2)


def test_batch_his_write_by_ids(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    test_pt_id1 = create_kw_pt_fn()
    test_pt_id2 = create_kw_pt_fn()

    ts_now = datetime.now(ZoneInfo("America/New_York"))

    rows = [
        {
            "ts": ts_now - timedelta(seconds=30),
            "v0": Number(72.2),
            "v1": Number(76.3),
        },
        {"ts": ts_now, "v0": Number(76.3), "v1": Number(72.2)},
    ]

    # write the his data to the test pt
    hc.his_write_by_ids([test_pt_id1, test_pt_id2], rows)

    range = date.today()
    his_grid = hc.his_read_by_ids([test_pt_id1, test_pt_id2], range)

    assert his_grid.rows[0]["v0"] == Number(72.19999694824219)
    assert his_grid.rows[1]["v0"] == Number(76.30000305175781)
    assert his_grid.rows[0]["v1"] == Number(76.30000305175781)
    assert his_grid.rows[1]["v1"] == Number(72.19999694824219)


def test_client_his_read_with_pandas(hc: Client):
    # We are importing pandas here only to check that it can be imported.
    # This can be improved in the future.
    pytest.importorskip("pandas")
    pts = hc.read("point and power and equipRef->siteMeter")
    pts_his_df = hc.his_read(pts, date.today()).to_pandas()

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


def test_client_his_read_by_ids_with_pandas(hc: Client):
    # We are importing pandas here only to check that it can be imported.
    # This can be improved in the future.
    pytest.importorskip("pandas")
    pts = hc.read("point and power and equipRef->siteMeter")
    pts_his_df = hc.his_read_by_ids(
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


def test_failed_commit(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    data = [{"id": pt_id, "dis": "TestRec", "testing": Marker(), "pytest": Marker()}]

    with pytest.raises(HaystackErrorGridResponseError):
        hc.commit(data, CommitFlag.ADD, False)


def test_single_commit(hc: Client):
    # create a new rec
    data = [{"dis": "TestRec", "testing": Marker(), "pytest": Marker()}]
    response: Grid = hc.commit(data, CommitFlag.ADD, False)

    new_rec_id = response.rows[0]["id"]
    mod = response.rows[0]["mod"]

    temp_rows = []
    for row in response.rows:
        del row["id"]
        del row["mod"]
        temp_rows.append(row)

    assert temp_rows == data

    assert isinstance(new_rec_id, Ref)
    assert isinstance(mod, datetime)

    assert response.rows[0]["dis"] == "TestRec"
    assert response.rows[0]["testing"] == Marker()
    assert response.rows[0]["pytest"] == Marker()

    # add a new tag called foo to the newly created rec
    # this time have the response return the full tag defs
    data = [{"id": new_rec_id, "mod": mod, "foo": "new tag"}]
    response: Grid = hc.commit(data, CommitFlag.UPDATE, True)

    new_rec_id = response.rows[0]["id"]
    mod = response.rows[0]["mod"]

    # verify the response
    assert isinstance(new_rec_id, Ref)
    assert isinstance(mod, datetime)
    assert response.rows[0]["dis"] == "TestRec"
    assert response.rows[0]["testing"] == Marker()
    assert response.rows[0]["pytest"] == Marker()
    assert response.rows[0]["foo"] == "new tag"

    # remove the newly created rec
    data = [{"id": new_rec_id, "mod": mod}]
    response: Grid = hc.commit(data, CommitFlag.REMOVE)

    # Test invalid Refs
    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(new_rec_id)


def test_batch_commit(hc: Client):
    data = [
        {"dis": "TestRec1", "testing1": Marker(), "pytest": Marker()},
        {"dis": "TestRec2", "testing2": Marker(), "pytest": Marker()},
    ]
    response: Grid = hc.commit(data, CommitFlag.ADD, False)

    new_rec_id1 = response.rows[0]["id"]
    mod1 = response.rows[0]["mod"]

    new_rec_id2 = response.rows[1]["id"]
    mod2 = response.rows[1]["mod"]

    temp_rows = []
    for row in response.rows:
        del row["id"]
        del row["mod"]
        temp_rows.append(row)

    assert temp_rows == data

    assert isinstance(new_rec_id1, Ref)
    assert isinstance(mod1, datetime)

    assert isinstance(new_rec_id2, Ref)
    assert isinstance(mod2, datetime)

    assert response.rows[0]["testing1"] == Marker()
    assert response.rows[1]["testing2"] == Marker()

    # add a new tag called foo to the newly created rec
    # this time have the response return the full tag defs
    data = [
        {"id": new_rec_id1, "mod": mod1, "foo": "new tag1"},
        {"id": new_rec_id2, "mod": mod2, "foo": "new tag2"},
    ]
    response: Grid = hc.commit(data, CommitFlag.UPDATE, True)

    new_rec_id1 = response.rows[0]["id"]
    mod1 = response.rows[0]["mod"]
    new_rec_id2 = response.rows[1]["id"]
    mod2 = response.rows[1]["mod"]

    assert response.rows[0]["foo"] == "new tag1"
    assert response.rows[1]["foo"] == "new tag2"

    data = [
        {"id": new_rec_id1, "mod": mod1},
        {"id": new_rec_id2, "mod": mod2},
    ]
    response: Grid = hc.commit(data, CommitFlag.REMOVE)

    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(new_rec_id1)

    with pytest.raises(HaystackReadOpUnknownRecError):
        response = hc.read_by_ids(new_rec_id2)


def test_point_write_number(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    response = hc.point_write(pt_id, 1, Number(0, "kW"))

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_number_who(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    response = hc.point_write(pt_id, 1, Number(50, "kW"), "Phable")

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []

    check_response = hc.point_write_array(pt_id)
    check_row = check_response.rows[0]

    assert check_row["val"] == Number(50, "kW")
    assert "Phable" in check_row["who"]
    assert "expires" not in check_row.keys()


def test_point_write_number_who_dur(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    response = hc.point_write(pt_id, 8, Number(100, "kW"), "Phable", Number(5, "min"))

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []

    check_response = hc.point_write_array(pt_id)
    check_row = check_response.rows[7]
    expires = check_row["expires"]

    assert check_row["val"] == Number(100, "kW")
    assert "Phable" in check_row["who"]
    assert expires.unit == "min"
    assert expires.val > 4.0 and expires.val < 5.0


def test_point_write_null(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    response = hc.point_write(pt_id, 1)

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_array(create_kw_pt_fn: Callable[[], Ref], hc: Client):
    pt_id = create_kw_pt_fn()
    response = hc.point_write_array(pt_id)

    assert response.rows[0]["level"] == Number(1)
    assert response.rows[-1]["level"] == Number(17)
