import logging
import sys

import pytest

import phable.scram as scram

logger = logging.getLogger(__name__)


def test_get_auth_token():
    host_url = "http://localhost:8080"
    username = "su"
    password = "su"

    for i in range(1):
        sc = scram.ScramClient(host_url, username, password)
        auth_token = sc.auth_token
        print(f"Here is the auth token: {auth_token}")
        assert len(auth_token) > 40
        assert "web-" in auth_token


def test_to_custom_hex():
    x_hex = scram._to_custom_hex(x=255, length=4)
    assert len(x_hex) == 4
    assert x_hex == "00ff"
    # add some more tests here...


def test_gen_nonce():
    nonce = scram._gen_nonce()
    assert len(nonce) == 32
    assert isinstance(nonce, str)
