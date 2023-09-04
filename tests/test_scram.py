from phable.auth.scram import _from_base64, _to_base64, _to_bytes

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
