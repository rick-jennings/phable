from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from phable.client import Client, CommitFlag, HaystackReadOpUnknownRecError
from phable.kinds import DateRange, DateTimeRange, Grid, Marker, Number, Ref

# Note 1:  These tests are made using SkySpark as the Haystack server
# Note 2:  These tests create pt records on the server with the pytest tag.
#          Probably you will want to manually delete these test pt recs.


@pytest.fixture
def hc() -> Client:
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    return Client(uri, username, password)


# -----------------------------------------------------------------------------
# auth tests
# -----------------------------------------------------------------------------


def test_open(hc: Client):
    hc._password = "wrong_password"  # type: ignore
    with pytest.raises(Exception):
        hc.open()


def test_auth_token(hc: Client):
    hc.open()
    auth_token = hc._auth_token  # type: ignore

    # verify the auth token is valid
    assert len(auth_token) > 40
    assert "web-" in auth_token

    # close the auth session
    hc.close()

    # # TODO:  verify the auth token used after close generates
    # # 403 error specifically
    # with pytest.raises(IncorrectHttpStatus):
    #     hc.about()


def test_context_manager(hc: Client):
    with hc:
        auth_token = hc._auth_token  # type: ignore

        # verify the auth token is valid
        assert len(auth_token) > 40
        assert "web-" in auth_token

        # verify the about op pre-close
        assert hc.about()["vendorName"] == "SkyFoundry"

    # # TODO:  verify the auth token used after close generates
    # # 403 error specifically
    # with pytest.raises(IncorrectHttpStatus):
    #     hc.about()


# -----------------------------------------------------------------------------
# haystack op tests
# -----------------------------------------------------------------------------


def test_about_op(hc: Client):
    hc.open()
    # verify the about op pre-close
    assert hc.about()["vendorName"] == "SkyFoundry"

    # close the auth session
    hc.close()


def test_read_site(hc: Client):
    with hc:
        grid = hc.read('site and dis=="Carytown"')
    # assert grid.iloc[0]["geoState"] == "VA"
    assert grid.rows[0]["geoState"] == "VA"


def test_read_UnknownRecError(hc: Client):
    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            hc.read("hi")


def test_read_point(hc: Client):
    with hc:
        grid = hc.read(
            """point and siteRef->dis=="Carytown" and """
            """equipRef->siteMeter and power"""
        )
    assert isinstance(grid.rows[0]["power"], Marker)


def test_read_by_id(hc: Client):
    """Test Client.read_by_ids() using a single id"""

    # Test a valid Ref
    with hc:
        id1 = hc.read("point and power and equipRef->siteMeter").rows[0]["id"]
        response = hc.read_by_ids(id1)
    assert response.rows[0]["navName"] == "kW"
    # Test an invalid Ref
    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(Ref("invalid-id"))


# TODO:  Come up with a better test than this
def test_read_by_ids(hc: Client):
    # Test valid Refs
    with hc:
        ids = hc.read("point and power and equipRef->siteMeter")
        id1 = ids.rows[0]["id"]
        id2 = ids.rows[1]["id"]

        response = hc.read_by_ids([id1, id2])

    assert response.rows[0]["tz"] == "New_York"
    assert response.rows[1]["tz"] == "New_York"

    # Test invalid Refs
    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(
                [Ref("p:demo:r:2c26ff0c-d04a5b02"), Ref("invalid-id")]
            )

    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(
                [Ref("invalid-id"), Ref("p:demo:r:2c26ff0c-0b8c49a1")]
            )

    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids([Ref("invalid-id1"), Ref("invalid-id2")])


def test_his_read_by_ids_with_date_range(hc: Client):
    with hc:
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
    with hc:
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
    with hc:
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
    with hc:
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
    with hc:
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
    with hc:
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


def test_single_his_write(hc: Client):
    with hc:
        # create a test point on the Haystack server and fetch the Ref ID
        axon_expr = """
            diff(null, {pytest, point, his, tz: "New_York",
                        kind: "Number"}, {add}).commit
        """
        test_pt_id = hc.eval(axon_expr).rows[0]["id"]

        ts_now = datetime.now(ZoneInfo("America/New_York"))

        meta = {"ver": "3.0", "id": test_pt_id}
        cols = [{"name": "ts"}, {"name": "val"}]
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

        his_grid = Grid(meta, cols, rows)

        # write the his data to the test pt
        hc.his_write(his_grid)

    with hc:
        range = date.today()
        his_grid = hc.his_read_by_ids(test_pt_id, range)

    assert his_grid.rows[0]["val"] == Number(72.19999694824219)
    assert his_grid.rows[1]["val"] == Number(76.30000305175781)


def test_batch_his_write(hc: Client):
    with hc:
        # create two test points on the Haystack server and fetch the Ref IDs
        axon_expr = """
            diff(null, {pytest, point, his, tz: "New_York",
                        kind: "Number"}, {add}).commit
        """
        test_pt_id1 = hc.eval(axon_expr).rows[0]["id"]
        test_pt_id2 = hc.eval(axon_expr).rows[0]["id"]

        ts_now = datetime.now(ZoneInfo("America/New_York"))

        meta = {"ver": "3.0"}
        cols = [
            {"name": "ts"},
            {"name": "v0", "meta": {"id": test_pt_id1}},
            {"name": "v1", "meta": {"id": test_pt_id2}},
        ]
        rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "v0": Number(72.2),
                "v1": Number(76.3),
            },
            {"ts": ts_now, "v0": Number(76.3), "v1": Number(72.2)},
        ]

        his_grid = Grid(meta, cols, rows)

        # write the his data to the test pt
        hc.his_write(his_grid)

    with hc:
        range = date.today()

        his_grid = hc.his_read_by_ids([test_pt_id1, test_pt_id2], range)

    assert his_grid.rows[0]["v0"] == Number(72.19999694824219)
    assert his_grid.rows[1]["v0"] == Number(76.30000305175781)
    assert his_grid.rows[0]["v1"] == Number(76.30000305175781)
    assert his_grid.rows[1]["v1"] == Number(72.19999694824219)

    # # delete the point rec from the server
    # rec = f"readById(@{test_pt_id1.val})"
    # axon_expr = f"commit(diff({rec}, null, {{remove}}))"
    # hc.eval(axon_expr)


def test_client_his_read_with_pandas(hc: Client):
    # We are importing pandas here only to check that it can be imported.
    # This can be improved in the future.
    pytest.importorskip("pandas")  # type: ignore
    with hc:
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
    pytest.importorskip("pandas")  # type: ignore
    with hc:
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


def test_single_commit(hc: Client):

    # create a new rec
    with hc:
        data = [{"dis": "TestRec", "testing": Marker(), "pytest": Marker()}]
        response: Grid = hc.commit(data, CommitFlag.ADD, False)

    new_rec_id = response.rows[0]["id"]
    mod = response.rows[0]["mod"]

    # verify the response
    # TODO: check why the server applies response return when not expected
    assert isinstance(new_rec_id, Ref)
    assert isinstance(mod, datetime)

    assert response.rows[0]["dis"] == "TestRec"
    assert response.rows[0]["testing"] == Marker()
    assert response.rows[0]["pytest"] == Marker()

    # add a new tag called foo to the newly created rec
    # this time have the response return the full tag defs
    with hc:
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
    with hc:
        data = [{"id": new_rec_id, "mod": mod}]
        response: Grid = hc.commit(data, CommitFlag.REMOVE)

    # Test invalid Refs
    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(new_rec_id)


def test_batch_commit(hc: Client):

    # create a new rec
    with hc:
        data = [
            {"dis": "TestRec1", "testing1": Marker(), "pytest": Marker()},
            {"dis": "TestRec2", "testing2": Marker(), "pytest": Marker()},
        ]
        response: Grid = hc.commit(data, CommitFlag.ADD, False)

    new_rec_id1 = response.rows[0]["id"]
    mod1 = response.rows[0]["mod"]

    new_rec_id2 = response.rows[1]["id"]
    mod2 = response.rows[1]["mod"]

    # verify the response
    # TODO: check why the server applies response return when not expected
    assert isinstance(new_rec_id1, Ref)
    assert isinstance(mod1, datetime)

    assert isinstance(new_rec_id2, Ref)
    assert isinstance(mod2, datetime)

    assert response.rows[0]["testing1"] == Marker()
    assert response.rows[1]["testing2"] == Marker()

    # add a new tag called foo to the newly created rec
    # this time have the response return the full tag defs
    with hc:
        data = [
            {"id": new_rec_id1, "mod": mod1, "foo": "new tag1"},
            {"id": new_rec_id2, "mod": mod2, "foo": "new tag2"},
        ]
        response: Grid = hc.commit(data, CommitFlag.UPDATE, True)

    new_rec_id1 = response.rows[0]["id"]
    mod1 = response.rows[0]["mod"]
    new_rec_id2 = response.rows[1]["id"]
    mod2 = response.rows[1]["mod"]

    # verify the response
    assert response.rows[0]["foo"] == "new tag1"
    assert response.rows[1]["foo"] == "new tag2"

    # remove the newly created recs
    with hc:
        data = [{"id": new_rec_id1, "mod": mod1}, {"id": new_rec_id2, "mod": mod2}]
        response: Grid = hc.commit(data, CommitFlag.REMOVE)

    # Test invalid Refs
    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(new_rec_id1)

    with pytest.raises(HaystackReadOpUnknownRecError):
        with hc:
            response = hc.read_by_ids(new_rec_id2)


def test_point_write_number(hc: Client):

    with hc:
        response = hc.point_write(Ref("2d6a2714-0d0a79fb"), 1, Number(0, "kW"))

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_number_who(hc: Client):

    with hc:
        response = hc.point_write(
            Ref("2d6a2714-0d0a79fb"), 1, Number(0, "kW"), "Phable"
        )

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_number_who_dur(hc: Client):

    with hc:
        response = hc.point_write(
            Ref("2d6a2714-0d0a79fb"),
            8,
            Number(0, "kW"),
            "Phable",  # , Number(0.5, "hr")
        )

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_null(hc: Client):

    with hc:
        response = hc.point_write(Ref("2d6a2714-0d0a79fb"), 1)

    assert isinstance(response, Grid)
    assert response.meta["ok"] == Marker()
    assert response.cols[0]["name"] == "empty"
    assert response.rows == []


def test_point_write_array(hc: Client):

    with hc:
        response = hc.point_write_array(Ref("2d6a2714-0d0a79fb"))

    assert response.rows[0]["level"] == Number(1)
    assert response.rows[-1]["level"] == Number(17)
