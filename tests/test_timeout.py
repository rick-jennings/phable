# flake8: noqa

from email.message import Message
from urllib.error import HTTPError

import pytest

from phable import AuthError, HaxallClient, HaystackClient

_URI = "http://localhost:8080/api/sys"
_USERNAME = "su"
_PASSWORD = "su"

_TIMEOUT_CASES = [
    ({"timeout": 5}, 5),
    ({}, 30),
    ({"timeout": None}, None),
]


def _open(open_kwargs):
    HaxallClient.open(_URI, _USERNAME, _PASSWORD, **open_kwargs)


def _call(open_kwargs):
    HaystackClient(_URI, "fake-auth-token", **open_kwargs).call("about")


def _file_get(open_kwargs):
    HaxallClient(_URI, "fake-auth-token", **open_kwargs).file_get(
        "/proj/demo/io/data.txt"
    )


_SCENARIOS = [
    (_open, AuthError),
    (_call, HTTPError),
    (_file_get, HTTPError),
]


@pytest.mark.parametrize(
    "action, expected_exception", _SCENARIOS, ids=["open", "call", "file_get"]
)
@pytest.mark.parametrize("open_kwargs, expected_timeout", _TIMEOUT_CASES)
def test_timeout_is_passed_to_urlopen(
    open_kwargs: dict,
    expected_timeout,
    action,
    expected_exception,
    mocker,
):
    mock_urlopen = mocker.patch(
        "urllib.request.urlopen",
        side_effect=HTTPError(_URI, 403, "Forbidden", Message(), None),
    )

    with pytest.raises(expected_exception):
        action(open_kwargs)

    assert mock_urlopen.call_args_list
    assert all(
        call.kwargs["timeout"] == expected_timeout
        for call in mock_urlopen.call_args_list
    )
