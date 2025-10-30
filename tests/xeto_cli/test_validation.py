from typing import Generator

import pytest

from phable import Grid, Number, Ref
from phable.kinds import PhKind

from .validate import validate_data


# @pytest.fixture(params=["json", "zinc"], scope="module")
@pytest.fixture(params=["json"], scope="module")
def io_format(request) -> Generator[str, None, None]:
    yield request.param


@pytest.mark.parametrize(
    "test_input,expected",
    [
        # test #1 w/o id passes
        (
            {
                "x": Number(2, "kW"),
                "y": Number(3, "kW"),
                "spec": Ref("phable::TestDict"),
            },
            Grid({"ver": "3.0"}, [{"name": "id"}, {"name": "msg"}], []),
        ),
        # test #1 w/ id passes
        (
            {
                "id": Ref("hi"),
                "x": Number(2, "kW"),
                "y": Number(3, "kW"),
                "spec": Ref("phable::TestDict"),
            },
            Grid({"ver": "3.0"}, [{"name": "id"}, {"name": "msg"}], []),
        ),
        # test #2 w/ id fails
        (
            {
                "id": Ref("hi"),
                "y": Number(3, "kW"),
                "spec": Ref("phable::TestDict"),
            },
            Grid(
                {"ver": "3.0"},
                [{"name": "id"}, {"name": "msg"}],
                [
                    {
                        "id": Ref("hi"),
                        "msg": "Slot 'x': Missing required slot",
                    }
                ],
            ),
        ),
        # test #3 w/o id fails
        (
            {
                "y": Number(3, "kW"),
                "spec": Ref("phable::TestDict"),
            },
            Grid(
                {"ver": "3.0"},
                [{"name": "id"}, {"name": "msg"}],
                [
                    {
                        "id": Ref("0"),
                        "msg": "Slot 'x': Missing required slot",
                    }
                ],
            ),
        ),
    ],
)
def test_validation(test_input: PhKind, expected: Grid, io_format: str) -> None:
    x = validate_data(test_input, io_format)
    assert x == expected
