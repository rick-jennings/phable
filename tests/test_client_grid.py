# from datetime import date, datetime, timedelta
# from zoneinfo import ZoneInfo

# import pytest

# from phable.client import Client, HaystackReadOpUnknownRecError
# from phable.kinds import DateRange, DateTimeRange, Grid, Marker, Number, Ref

# # Note 1:  These tests are made using SkySpark as the Haystack server
# # Note 2:  These tests create pt records on the server with the pytest tag.
# #          Probably you will want to manually delete these test pt recs.


# @pytest.fixture
# def hc() -> Client:
#     uri = "http://localhost:8080/api/demo1"
#     username = "su"
#     password = "su"

#     return Client(uri, username, password)


# # -----------------------------------------------------------------------------
# # auth tests
# # -----------------------------------------------------------------------------


# def test_open(hc: Client):
#     hc._password = "wrong_password"  # type: ignore
#     with pytest.raises(Exception):
#         hc.open()


# def test_auth_token(hc: Client):
#     hc.open()
#     auth_token = hc._auth_token  # type: ignore

#     # verify the auth token is valid
#     assert len(auth_token) > 40
#     assert "web-" in auth_token

#     # close the auth session
#     hc.close()

#     # # TODO:  verify the auth token used after close generates
#     # # 403 error specifically
#     # with pytest.raises(IncorrectHttpStatus):
#     #     hc.about()


# def test_context_manager(hc: Client):
#     with hc:
#         auth_token = hc._auth_token  # type: ignore

#         # verify the auth token is valid
#         assert len(auth_token) > 40
#         assert "web-" in auth_token

#         # verify the about op pre-close
#         assert hc.about().rows[0]["vendorName"] == "SkyFoundry"

#     # # TODO:  verify the auth token used after close generates
#     # # 403 error specifically
#     # with pytest.raises(IncorrectHttpStatus):
#     #     hc.about()


# # -----------------------------------------------------------------------------
# # haystack op tests
# # -----------------------------------------------------------------------------


# def test_about_op(hc: Client):
#     hc.open()
#     # verify the about op pre-close
#     assert hc.about().rows[0]["vendorName"] == "SkyFoundry"

#     # close the auth session
#     hc.close()


# def test_read_site(hc: Client):
#     with hc:
#         grid = hc.read('site and dis=="Carytown"')
#     assert grid.rows[0]["geoState"] == "VA"


# def test_read_point(hc: Client):
#     with hc:
#         grid = hc.read(
#             """point and siteRef->dis=="Carytown" and """
#             """equipRef->siteMeter and power"""
#         )
#     assert isinstance(grid.rows[0]["power"], Marker)


# def test_read_by_id(hc: Client):
#     # Test a valid Ref
#     with hc:
#         id1 = hc.read("point and power and equipRef->siteMeter").rows[0]["id"]
#         response = hc.read_by_id(id1)
#     assert response.rows[0]["navName"] == "kW"

#     # Test an invalid Ref
#     with pytest.raises(HaystackReadOpUnknownRecError):
#         with hc:
#             response = hc.read_by_id(Ref("invalid-id"))


# # TODO:  Come up with a better test than this
# def test_read_by_ids(hc: Client):
#     # Test valid Refs
#     with hc:
#         ids = hc.read("point and power and equipRef->siteMeter")
#         id1 = ids.rows[0]["id"]
#         id2 = ids.rows[1]["id"]

#         response = hc.read_by_ids([id1, id2])

#     for row in response.rows:
#         assert row["tz"] == "New_York"

#     # Test invalid Refs
#     with pytest.raises(HaystackReadOpUnknownRecError):
#         with hc:
#             response = hc.read_by_ids(
#                 [Ref("p:demo:r:2c26ff0c-d04a5b02"), Ref("invalid-id")]
#             )

#     with pytest.raises(HaystackReadOpUnknownRecError):
#         with hc:
#             response = hc.read_by_ids(
#                 [Ref("invalid-id"), Ref("p:demo:r:2c26ff0c-0b8c49a1")]
#             )

#     with pytest.raises(HaystackReadOpUnknownRecError):
#         with hc:
#             response = hc.read_by_ids([Ref("invalid-id1"), Ref("invalid-id2")])


# def test_his_read_with_date_range(hc: Client):
#     with hc:
#         # find the point id
#         point_grid = hc.read(
#             """point and siteRef->dis=="Carytown" and """
#             """equipRef->siteMeter and power"""
#         )
#         point_ref = point_grid.rows[0]["id"]

#         # get the his using Date as the range
#         start = date.today() - timedelta(days=7)
#         his_grid = hc.his_read(point_ref, start)

#     # check his_grid
#     cols = [col["name"] for col in his_grid.cols]
#     assert isinstance(his_grid.rows[0][cols[1]], Number)
#     assert his_grid.rows[0][cols[1]].unit == "kW"
#     assert his_grid.rows[0][cols[1]].val >= 0
#     assert his_grid.rows[0][cols[0]].date() == start
#     assert his_grid.rows[-1][cols[0]].date() == start


# def test_his_read_with_datetime_range(hc: Client):
#     with hc:
#         # find the point id
#         point_grid = hc.read(
#             """point and siteRef->dis=="Carytown" and """
#             """equipRef->siteMeter and power"""
#         )
#         point_ref = point_grid.rows[0]["id"]

#         # get the his using Date as the range
#         datetime_range = DateTimeRange(
#             datetime(
#                 2023, 8, 20, 10, 12, 12, tzinfo=ZoneInfo("America/New_York")
#             )
#         )
#         his_grid = hc.his_read(point_ref, datetime_range)

#     # check his_grid
#     cols = [col["name"] for col in his_grid.cols]
#     assert isinstance(his_grid.rows[0][cols[1]], Number)
#     assert his_grid.rows[0][cols[1]].unit == "kW"
#     assert his_grid.rows[0][cols[1]].val >= 0
#     assert his_grid.rows[0][cols[0]].date() == datetime_range.start.date()
#     assert his_grid.rows[-1][cols[0]].date() == date.today()


# def test_his_read_with_date_slice(hc: Client):
#     with hc:
#         # find the point id
#         point_grid = hc.read(
#             """point and siteRef->dis=="Carytown" and """
#             """equipRef->siteMeter and power"""
#         )
#         point_ref = point_grid.rows[0]["id"]

#         # get the his using Date as the range
#         start = date.today() - timedelta(days=7)
#         end = date.today()
#         date_range = DateRange(start, end)
#         his_grid = hc.his_read(point_ref, date_range)

#     # check his_grid
#     cols = [col["name"] for col in his_grid.cols]
#     assert isinstance(his_grid.rows[0][cols[1]], Number)
#     assert his_grid.rows[0][cols[1]].unit == "kW"
#     assert his_grid.rows[0][cols[1]].val >= 0
#     assert his_grid.rows[0][cols[0]].date() == start
#     assert his_grid.rows[-1][cols[0]].date() == end


# def test_his_read_with_datetime_slice(hc: Client):
#     with hc:
#         # find the point id
#         point_grid = hc.read(
#             """point and siteRef->dis=="Carytown" and """
#             """equipRef->siteMeter and power"""
#         )
#         point_ref = point_grid.rows[0]["id"]

#         # get the his using Date as the range
#         start = datetime(
#             2023, 8, 20, 12, 12, 23, tzinfo=ZoneInfo("America/New_York")
#         )
#         end = start + timedelta(days=3)

#         datetime_range = DateTimeRange(start, end)

#         his_grid = hc.his_read(point_ref, datetime_range)

#     # check his_grid
#     cols = [col["name"] for col in his_grid.cols]
#     assert isinstance(his_grid.rows[0][cols[1]], Number)
#     assert his_grid.rows[0][cols[1]].unit == "kW"
#     assert his_grid.rows[0][cols[1]].val >= 0
#     assert his_grid.rows[0][cols[0]].date() == start.date()
#     assert his_grid.rows[-1][cols[0]].date() == end.date()


# def test_batch_his_read(hc: Client):
#     with hc:
#         ids = hc.read("point and power and equipRef->siteMeter")
#         id1 = ids.rows[0]["id"]
#         id2 = ids.rows[1]["id"]
#         id3 = ids.rows[2]["id"]
#         id4 = ids.rows[3]["id"]

#         ids = [id1, id2, id3, id4]
#         his_grid = hc.his_read(ids, date.today())

#     cols = [col["name"] for col in his_grid.cols]
#     assert isinstance(his_grid.rows[0][cols[0]], datetime)
#     assert isinstance(his_grid.rows[0][cols[1]], Number)
#     assert his_grid.rows[0][cols[1]].unit == "kW"
#     assert his_grid.rows[0][cols[1]].val >= 0
#     assert isinstance(his_grid.rows[0][cols[4]], Number)
#     assert his_grid.rows[0][cols[4]].unit == "kW"
#     assert his_grid.rows[0][cols[4]].val >= 0


# def test_single_his_write(hc: Client):
#     with hc:
#         # create a test point on the Haystack server and fetch the Ref ID
#         axon_expr = """
#             diff(null, {pytest, point, his, tz: "New_York",
#                         kind: "Number"}, {add}).commit
#         """
#         test_pt_id = hc.eval(axon_expr).rows[0]["id"]

#         ts_now = datetime.now(ZoneInfo("America/New_York"))

#         meta = {"ver": "3.0", "id": test_pt_id}
#         cols = [{"name": "ts"}, {"name": "val"}]
#         rows = [
#             {
#                 "ts": ts_now - timedelta(seconds=30),
#                 "val": Number(72.2),
#             },
#             {
#                 "ts": ts_now,
#                 "val": Number(76.3),
#             },
#         ]

#         his_grid = Grid(meta, cols, rows)

#         # write the his data to the test pt
#         response_grid = hc.his_write(his_grid)

#     assert "err" not in response_grid.meta.keys()

#     with hc:
#         range = date.today()

#         response_grid = hc.his_read(test_pt_id, range)

#     assert response_grid.rows[0]["val"] == 72.19999694824219
#     assert response_grid.rows[1]["val"] == 76.30000305175781


# def test_batch_his_write(hc: Client):
#     with hc:
#         # create two test points on the Haystack server and fetch the Ref IDs
#         axon_expr = """
#             diff(null, {pytest, point, his, tz: "New_York",
#                         kind: "Number"}, {add}).commit
#         """
#         test_pt_id1 = hc.eval(axon_expr).rows[0]["id"]
#         test_pt_id2 = hc.eval(axon_expr).rows[0]["id"]

#         ts_now = datetime.now(ZoneInfo("America/New_York"))

#         meta = {"ver": "3.0"}
#         cols = [
#             {"name": "ts"},
#             {"name": "v0", "meta": {"id": test_pt_id1}},
#             {"name": "v1", "meta": {"id": test_pt_id2}},
#         ]
#         rows = [
#             {
#                 "ts": ts_now - timedelta(seconds=30),
#                 "v0": Number(72.2),
#                 "v1": Number(76.3),
#             },
#             {"ts": ts_now, "v0": Number(76.3), "v1": Number(72.2)},
#         ]

#         his_grid = Grid(meta, cols, rows)

#         # write the his data to the test pt
#         response_grid = hc.his_write(his_grid)

#     assert "err" not in response_grid.meta.keys()

#     with hc:
#         range = date.today()

#         response_grid = hc.his_read([test_pt_id1, test_pt_id2], range)

#     assert response_grid.rows[0]["v0"] == 72.19999694824219
#     assert response_grid.rows[1]["v0"] == 76.30000305175781
#     assert response_grid.rows[0]["v1"] == 76.30000305175781
#     assert response_grid.rows[1]["v1"] == 72.19999694824219

#     # # delete the point rec from the server
#     # rec = f"readById(@{test_pt_id1.val})"
#     # axon_expr = f"commit(diff({rec}, null, {{remove}}))"
#     # hc.eval(axon_expr)
