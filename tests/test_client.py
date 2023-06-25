import logging
import sys

import pytest

from phable.client import Client, IncorrectHttpStatus
from phable.exceptions import UnknownRecError
from phable.kinds import DateTime, Grid, Marker, Number, Ref

logger = logging.getLogger(__name__)

# Note 1:  These tests are made using SkySpark as the Haystack server
# Note 2:  Fix "p:demo:r:2bf586e4-7cd2c9c4".  Maybe make some fake rec/his and use throughout test script.


@pytest.fixture
def hc() -> Client:
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    return Client(uri, username, password)


# --------------------------------------------------------------------------------------
# auth tests
# --------------------------------------------------------------------------------------


def test_open(hc: Client):
    hc._password = "wrong_password"
    with pytest.raises(Exception):
        hc.open()


def test_auth_token(hc: Client):
    hc.open()
    auth_token = hc._auth_token

    # verify the auth token is valid
    assert len(auth_token) > 40
    assert "web-" in auth_token

    # close the auth session
    hc.close()

    # TODO:  verify the auth token used after close generates
    # 403 error specifically
    with pytest.raises(IncorrectHttpStatus):
        hc.about()


def test_context_manager(hc: Client):
    with hc:
        auth_token = hc._auth_token

        # verify the auth token is valid
        assert len(auth_token) > 40
        assert "web-" in auth_token

        # verify the about op pre-close
        assert hc.about()["vendorName"] == "SkyFoundry"

    # TODO:  verify the auth token used after close generates
    # 403 error specifically
    with pytest.raises(IncorrectHttpStatus):
        hc.about()


# --------------------------------------------------------------------------------------
# haystack op tests
# --------------------------------------------------------------------------------------


def test_about_op(hc: Client):
    hc.open()
    # verify the about op pre-close
    assert hc.about()["vendorName"] == "SkyFoundry"

    # close the auth session
    hc.close()


def test_read_site(hc: Client):
    with hc:
        grid = hc.read('site and dis=="Carytown"')
    assert grid.rows[0]["geoState"] == "VA"


def test_read_point(hc: Client):
    with hc:
        grid = hc.read(
            'point and siteRef->dis=="Carytown" and equipRef->siteMeter and power'
        )
    assert isinstance(grid.rows[0]["power"], Marker)


def test_read_by_id(hc: Client):
    # Test a valid Ref
    with hc:
        response = hc.read_by_id(Ref("p:demo:r:2c26ff0c-d04a5b02"))
    assert response["navName"] == "kW"

    # Test an invalid Ref
    with pytest.raises(UnknownRecError):
        with hc:
            response = hc.read_by_id(Ref("invalid-id"))


# TODO:  Come up with a better test than this
def test_read_by_ids(hc: Client):
    # Test valid Refs
    with hc:
        response = hc.read_by_ids(
            [Ref("p:demo:r:2c26ff0c-d04a5b02"), Ref("p:demo:r:2c26ff0c-0b8c49a1")]
        )

    for row in response.rows:
        assert row["tz"] == "New_York"

    # Test invalid Refs
    with pytest.raises(UnknownRecError):
        with hc:
            response = hc.read_by_ids(
                [Ref("p:demo:r:2c26ff0c-d04a5b02"), Ref("invalid-id")]
            )

    with pytest.raises(UnknownRecError):
        with hc:
            response = hc.read_by_ids(
                [Ref("invalid-id"), Ref("p:demo:r:2c26ff0c-0b8c49a1")]
            )

    with pytest.raises(UnknownRecError):
        with hc:
            response = hc.read_by_ids([Ref("invalid-id1"), Ref("invalid-id2")])


# TODO:  Change the date used in the test to be relative.
# TODO:  Find some better data value checks
# NOTE:  This code below is temporary and will eventually break!
def test_his_read(hc: Client):
    with hc:
        # find the point id
        point_grid = hc.read(
            'point and siteRef->dis=="Carytown" and equipRef->siteMeter and power'
        )
        point_ref = point_grid.rows[0]["id"]
        # get the his
        his_grid = hc.his_read(point_ref, "2023-05-02")

    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0


def test_batch_his_read(hc: Client):
    with hc:
        ids = [
            Ref("p:demo:r:2c26ff0c-d04a5b02"),
            Ref("p:demo:r:2c26ff0c-0b8c49a1"),
            Ref("p:demo:r:2c26ff0c-7fdb626d"),
            Ref("p:demo:r:2c26ff0c-1773db74"),
        ]
        his_grid = hc.his_read(ids, "2023-05-02")

    cols = [col["name"] for col in his_grid.cols]
    assert isinstance(his_grid.rows[0][cols[0]], DateTime)
    assert isinstance(his_grid.rows[0][cols[1]], Number)
    assert his_grid.rows[0][cols[1]].unit == "kW"
    assert his_grid.rows[0][cols[1]].val >= 0
    assert isinstance(his_grid.rows[0][cols[4]], Number)
    assert his_grid.rows[0][cols[4]].unit == "kW"
    assert his_grid.rows[0][cols[4]].val >= 0


# TODO:  Manually create a point using the eval op since we know the ids will change
# NOTE:  This code below is temporary and will eventually break!
def test_his_write(hc: Client):
    with hc:
        his_grid = Grid(
            meta={
                "ver": "3.0",
                "id": {"_kind": "ref", "val": "p:demo:r:2c2b5ffb-97770806"},
            },
            cols=[{"name": "ts"}, {"name": "val"}],
            rows=[
                {
                    "ts": {
                        "_kind": "dateTime",
                        "val": "2023-05-01T00:00:00-05:00",
                        "tz": "New_York",
                    },
                    "val": {"_kind": "number", "val": 89, "unit": "kW"},
                }
            ],
        )
        grid = hc.his_write(his_grid)

    assert "err" not in grid.meta.keys()
    # TODO:  Now read what was written and verify it is correct
