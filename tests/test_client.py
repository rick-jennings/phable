import logging
import sys
from phable.kinds import Marker, Number, Grid
import pytest

from phable.client import Client, IncorrectHttpStatus

logger = logging.getLogger(__name__)

# Note:  These tests are made using SkySpark as the Haystack server


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

    assert isinstance(his_grid.rows[0]["val"], Number)
    assert his_grid.rows[0]["val"].unit == "kW"
    assert his_grid.rows[0]["val"].val >= 0


# TODO:  Manually create a point using the eval op since we know the ids will change
# NOTE:  This code below is temporary and will eventually break!
def test_his_write(hc: Client):
    with hc:
        his_grid = Grid(
            meta={
                "ver": "3.0",
                "id": {"_kind": "ref", "val": "p:demo:r:2bf586e4-7cd2c9c4"},
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
