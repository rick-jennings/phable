import logging

import pytest

from phable.kinds import Grid
from phable.phable import Phable

logger = logging.getLogger(__name__)


@pytest.fixture
def phable() -> Phable:
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    return Phable(uri=uri, username=username, password=password)


def test_about_op(phable: Phable):
    with phable as ph:
        about_grid = ph.about()

    assert about_grid.rows[0]["vendorName"] == "SkyFoundry"


def test_error_resp(phable: Phable):
    "Send an incorrect request grid and verify a valid response with error Grid."
    with phable as ph:
        request_dict = {
            "error_test": "",
            "limit": 3,
        }
        request_grid = Grid.to_grid(request_dict)
        response_grid = ph.call("read", "POST", request_grid)

    assert response_grid["meta"]["err"]["_kind"] == "marker"


# # TODO: Finish this test
# def test_incomplete_resp(phable: Phable):
#     with phable as ph:
#         # get a point id
#         grid = ph.read("point", 3)


# TODO:  Manually create a point using the eval op since we know the ids will change
def test_his_write(phable: Phable):
    with phable as ph:
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
        ph.his_write(his_grid)

    # TODO:  Now read what was written and verify it is correct
