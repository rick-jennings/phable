from email.message import Message
from unittest.mock import patch

import pytest

from phable.auth.scram import (
    ScramScheme,
    ScramServerResponseParsingError,
    _from_base64,
    _redact_auth_token,
    _to_base64,
    _to_bytes,
)
from phable.http import PhHttpResponse

# from phable.exceptions import NotFoundError


# def test__parse_hello_call_result():
#     data = "scram data=cj0xODI2YzEwY2VlZDMxYWNjOWYyYmFiY2IxMDAzZ\
# jdiNTUyNjhhOWFkYTk2NGRhNzhlYmNmYzAxOWIyY2ViNTVkLHM9d1luT3FYc1VTMUZKRHpwTmN3K09FQk9OV3lS\
# TWJMYUFrWkpCVUtnZ3RIMD0saT0xMDAwMA, handshakeToken=c3U, hash=SHA-256"

#     handshake_token, hash = parse_hello_call_result(data)
#     assert handshake_token == "c3U"
#     assert hash == "sha256"

#     with pytest.raises(NotFoundError):
#         parse_hello_call_result("This is an invalid input!")


# def test__parse_first_call_result():
#     data = "scram data=cj0xODI2YzEwY2VlZDMxYWNjOWYyYmFiY2IxMDAzZjdiNT\
# UyNjhhOWFkYTk2NGRhNzhlYmNmYzAxOWIyY2ViNTVkLHM9d1luT3FYc1VTMUZKRHpwTmN3K09FQk9OV3lSTWJMY\
# UFrWkpCVUtnZ3RIMD0saT0xMDAwMA, handshakeToken=c3U, hash=SHA-256"

#     assert parse_first_call_result(data) == (
#         "1826c10ceed31acc9f2babcb1003f7b55268a9ada964da78ebcfc019b2ceb55d",
#         "wYnOqXsUS1FJDzpNcw+OEBONWyRMbLaAkZJBUKggtH0=",
#         10000,
#     )

#     with pytest.raises(NotFoundError):
#         parse_first_call_result("This is an invalid input!")


def _make_scram() -> ScramScheme:
    return ScramScheme("http://localhost:8080/api/demo", "su", "su", "text/zinc")


def _make_res(status: int, headers: dict[str, str]) -> PhHttpResponse:
    msg = Message()
    for name, value in headers.items():
        msg[name] = value
    return PhHttpResponse(body=b"", headers=msg, status=status)


def test__hello_call_parses_scram_challenge():
    scram = _make_scram()
    res = _make_res(
        401, {"WWW-Authenticate": "scram handshakeToken=aabbcc, hash=SHA-256"}
    )

    with patch.object(ScramScheme, "_ph_scram_get", return_value=res):
        scram._hello_call()

    assert scram._handshake_token == "aabbcc"
    assert scram._hash == "SHA-256"


def test__hello_call_raises_on_non_challenge_response():
    scram = _make_scram()
    res = _make_res(200, {"Content-Type": "text/html", "Server": "some-proxy"})

    with patch.object(ScramScheme, "_ph_scram_get", return_value=res):
        with pytest.raises(ScramServerResponseParsingError) as e:
            scram._hello_call()

    assert "HTTP 200" in e.value.help_msg
    assert "some-proxy" in e.value.help_msg
    assert "http://localhost:8080/api/demo" in e.value.help_msg


def test__hello_call_raises_on_unparseable_challenge():
    scram = _make_scram()
    res = _make_res(401, {"WWW-Authenticate": "Basic realm=other-server"})

    with patch.object(ScramScheme, "_ph_scram_get", return_value=res):
        with pytest.raises(ScramServerResponseParsingError) as e:
            scram._hello_call()

    assert "HTTP 401" in e.value.help_msg
    assert "Basic realm=other-server" in e.value.help_msg


def test__first_call_raises_on_non_challenge_response():
    scram = _make_scram()
    scram._handshake_token = "aabbcc"
    res = _make_res(502, {"Content-Type": "text/html"})

    with patch.object(ScramScheme, "_ph_scram_get", return_value=res):
        with pytest.raises(ScramServerResponseParsingError) as e:
            scram._first_call()

    assert "HTTP 502" in e.value.help_msg


def test__redact_auth_token():
    msg = Message()
    msg["Authentication-Info"] = (
        "authToken=web-secret123, data=dj1abc, hashFunc=SHA-256"
    )
    msg["Content-Type"] = "text/zinc"

    redacted = _redact_auth_token(msg)

    assert "web-secret123" not in redacted["Authentication-Info"]
    assert "authToken=<redacted>" in redacted["Authentication-Info"]
    assert redacted["Content-Type"] == "text/zinc"


def test__to_base64():
    assert _to_base64("example") == "ZXhhbXBsZQ"
    assert _to_base64(bytes("example", "utf-8")) == "ZXhhbXBsZQ"


def test__from_base64():
    assert _from_base64("ZXhhbXBsZQ") == "example"


def test__to_bytes():
    assert _to_bytes("abcd") == b"abcd"
    assert _to_bytes("abcde") == b"abcde==="
    assert _to_bytes("abcdef") == b"abcdef=="
    assert _to_bytes("abcdefg") == b"abcdefg="
