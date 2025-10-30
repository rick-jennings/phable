from typing import Generator

import pytest

from phable import Grid, Number, Ref
from phable.kinds import PhKind

from .validate import validate_data


@pytest.fixture(params=["json", "zinc"], scope="module")
def io_format(request) -> Generator[str, None, None]:
    yield request.param


@pytest.mark.parametrize(
    "test_input,expected",
    [
        # phable::TestDict
        (
            {
                "x": Number(2, "kW"),
                "y": Number(3, "kW"),
                "spec": Ref("phable::TestDict"),
            },
            Grid({"ver": "3.0"}, [{"name": "id"}, {"name": "msg"}], []),
        ),
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
        # phable::TestNestedDict
        (
            {
                "a": Number(2, "kW"),
                "b": {"x": Number(2, "kW"), "y": Number(3, "kW")},
                "spec": Ref("phable::TestNestedDict"),
            },
            Grid({"ver": "3.0"}, [{"name": "id"}, {"name": "msg"}], []),
        ),
        (
            {
                "a": Number(2, "kW"),
                "b": {"y": Number(3, "kW")},
                "spec": Ref("phable::TestNestedDict"),
            },
            Grid(
                {"ver": "3.0"},
                [{"name": "id"}, {"name": "msg"}],
                [
                    {
                        # "id": Ref("0"),
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
