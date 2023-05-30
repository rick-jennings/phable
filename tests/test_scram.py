from phable.auth.scram import (
    _parse_scram_data,
    _parse_handshake_token,
    _parse_hash_func,
    _to_base64,
    _from_base64,
    _to_bytes,
)
from phable.exceptions import NotFoundError
import pytest


def test__parse_scram_data():
    scram_data = "scram data=cj0xODI2YzEwY2VlZDMxYWNjOWYyYmFiY2IxMDAzZjdiNT\
UyNjhhOWFkYTk2NGRhNzhlYmNmYzAxOWIyY2ViNTVkLHM9d1luT3FYc1VTMUZKRHpwTmN3K09FQk9OV3lSTWJMY\
UFrWkpCVUtnZ3RIMD0saT0xMDAwMA, handshakeToken=c3U, hash=SHA-256"

    assert _parse_scram_data(scram_data) == (
        "1826c10ceed31acc9f2babcb1003f7b55268a9ada964da78ebcfc019b2ceb55d",
        "wYnOqXsUS1FJDzpNcw+OEBONWyRMbLaAkZJBUKggtH0=",
        10000,
    )

    with pytest.raises(NotFoundError):
        _parse_scram_data("This is an invalid input!")


def test__parse_handshake_token():
    scram_data = "scram data=cj0xODI2YzEwY2VlZDMxYWNjOWYyYmFiY2IxMDAzZ\
jdiNTUyNjhhOWFkYTk2NGRhNzhlYmNmYzAxOWIyY2ViNTVkLHM9d1luT3FYc1VTMUZKRHpwTmN3K09FQk9OV3lS\
TWJMYUFrWkpCVUtnZ3RIMD0saT0xMDAwMA, handshakeToken=c3U, hash=SHA-256"

    assert _parse_handshake_token(scram_data) == "c3U"

    with pytest.raises(NotFoundError):
        _parse_handshake_token("This is an invalid input!")


def test__parse_hash_func():
    auth_header = "authToken=web-syPGBhoPY0XhKi6EXUG62BMACc0Ot7xuq4PShtj\
I47c-38,data=dj1ENDJEbS9kckRiSUN1NXpvTHd2OWloSlJiWkxzMFBRNllibm5EY2NNU1M4PQ,\
hash=SHA-256"

    assert _parse_hash_func(auth_header) == "sha256"

    with pytest.raises(NotFoundError):
        _parse_hash_func("This is an invalid input!")


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
