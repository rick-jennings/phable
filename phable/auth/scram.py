"""SCRAM Authentication Support for a Project Haystack Client

This module implements client-side logic and support for the following:
    1. Initiation of an authentication exchange according to Project Haystack
    2. SCRAM authentication according to RFC5802

Note:   HTTP messages are not sent by code within this module.  The Scram class and
        functions defined in this module are used in HTTP messages sent and received by
        the Client class in `phable.client.py`.
"""

import hashlib
import hmac
import re
from base64 import urlsafe_b64decode, urlsafe_b64encode, b64encode
from dataclasses import dataclass
from hashlib import pbkdf2_hmac
from functools import cached_property
from phable.exceptions import NotFoundError
from uuid import uuid4


from phable.http import HttpResponse


@dataclass(frozen=True)
class Scram:
    """Data container for RFC5802 SCRAM exchange with helper properties."""

    password: str
    hash: str
    handshake_token: str
    c1_bare: str
    s_nonce: str
    salt: str
    iter_count: int

    @cached_property
    def _salted_password(self) -> bytes:
        return _salted_password(self.salt, self.iter_count, self.hash, self.password)

    @property
    def client_key(self) -> bytes:
        return _hmac(self._salted_password, b"Client Key", self.hash)

    @property
    def stored_key(self) -> bytes:
        hashFunc = hashlib.new(self.hash)
        hashFunc.update(self.client_key)
        return hashFunc.digest()

    @property
    def client_final_no_proof(self) -> str:
        return f"c={to_base64('n,,')},r={self.s_nonce}"

    @property
    def auth_message(self) -> str:
        return (
            f"{self.c1_bare},r={self.s_nonce},s={self.salt},"
            + f"i={self.iter_count},{self.client_final_no_proof}"
        )

    @property
    def client_signature(self) -> bytes:
        return _hmac(self.stored_key, self.auth_message.encode("utf-8"), self.hash)

    @property
    def client_proof(self) -> str:
        return to_base64(_xor(self.client_key, self.client_signature))

    @property
    def server_key(self) -> bytes:
        return _hmac(self._salted_password, b"Server Key", self.hash)

    @property
    def server_signature(self) -> str:
        server_signature = _hmac(
            self.server_key, self.auth_message.encode("utf-8"), self.hash
        )
        server_signature = b64encode(server_signature).decode("utf-8")
        return server_signature

    @property
    def client_final_message(self) -> str:
        client_final = self.client_final_no_proof + ",p=" + self.client_proof
        return to_base64(client_final)


def parse_hello_call_result(hello_call_result: HttpResponse) -> tuple[str, str]:
    """Parses the handshake token and hash from the 'WWW-Authenticate' header in the
    server generated HELLO message.
    """

    auth_header = hello_call_result.headers["WWW-Authenticate"]

    # find the handshake token
    exclude_msg = "handshakeToken="
    s = re.search(f"({exclude_msg})[a-zA-Z0-9]+", auth_header)

    if s is None:
        raise NotFoundError(
            (
                "Handshake token not found in the 'WWW-Authenticate' header:"
                + f"\n{auth_header}"
            )
        )

    handshake_token = s.group(0)[len(exclude_msg) :]

    # find the hash
    exclude_msg = "hash="
    s = re.search(f"({exclude_msg})(SHA-256)", auth_header)

    if s is None:
        raise NotFoundError(
            f"Hash method not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )

    s_new = s.group(0)[len(exclude_msg) :]

    if s_new == "SHA-256":
        s_new = "sha256"

    hash = s_new

    return handshake_token, hash


def parse_first_call_result(first_call_result: HttpResponse) -> tuple[str, str, int]:
    """Parses the server nonce, salt, and iteration count from the 'WWW-Authenticate'
    header in the "server-first-message".
    """

    auth_header = first_call_result.headers["WWW-Authenticate"]
    exclude_msg = "scram data="
    scram_data = re.search(f"({exclude_msg})[a-zA-Z0-9]+", auth_header)

    if scram_data is None:
        raise NotFoundError(
            f"Scram data not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )

    decoded_scram_data = _from_base64(scram_data.group(0)[len(exclude_msg) :])
    s_nonce, salt, iteration_count = decoded_scram_data.replace(" ", "").split(",")

    if "r=" not in s_nonce:
        raise NotFoundError(
            f"Server nonce not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )
    elif "s=" not in salt:
        raise NotFoundError(
            f"Salt not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )
    elif "i=" not in iteration_count:
        raise NotFoundError(
            (
                "Iteration count not found in the 'WWW-Authenticate' header:"
                f"\n{auth_header}"
            )
        )

    return (
        s_nonce.replace("r=", "", 1),
        salt.replace("s=", "", 1),
        int(iteration_count.replace("i=", "", 1)),
    )


def parse_final_call_result(resp: HttpResponse) -> tuple[str, str]:
    """Parses the auth token from the 'WWW-Authenticate' header in the
    "server-final-message".
    """

    auth_header = resp.headers.as_string()

    exclude_msg1 = "authToken="
    s1 = re.search(f"({exclude_msg1})[^,]+", auth_header)

    if s1 is None:
        raise NotFoundError(
            f"Auth token not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )

    auth_token = s1.group(0)[len(exclude_msg1) :]

    exclude_msg2 = "data="
    s2 = re.search(f"({exclude_msg2})[^,]+", auth_header)

    data = s2.group(0)[len(exclude_msg2) :]

    server_signature = _from_base64(data).replace("v=", "", 1)

    return auth_token, server_signature


def c1_bare(username: str) -> str:
    nonce = str(uuid4()).replace("-", "")
    return f"n={username},r={nonce}"


def to_base64(msg: str | bytes) -> str:
    """Perform base64uri encoding of a message as defined by RFC 4648.

    Args:
        msg (str | bytes): A message to be encoded.

    Returns:
        str: A base64uri encoded message
    """

    # Convert str inputs to bytes
    if isinstance(msg, str):
        msg = msg.encode("utf-8")

    # Encode using URL and filesystem-safe alphabet.
    # This means + is encoded as -, and / is encoded as _.
    output = urlsafe_b64encode(msg)

    # Decode the output as a str
    output = output.decode("utf-8")

    # Remove padding
    output = output.replace("=", "")

    return output


def _salted_password(
    salt: str, iterations: int, hash_func: str, password: str
) -> bytes:
    """Generates a salted password according to RFC5802."""
    dk = pbkdf2_hmac(hash_func, password.encode(), urlsafe_b64decode(salt), iterations)
    return dk


def _from_base64(msg: str) -> str:  # str
    return urlsafe_b64decode(_to_bytes(msg)).decode("utf-8")


def _to_bytes(s: str) -> bytes:
    """Convert a string to a bytes object.

    Prior to conversion to bytes the string object must have a length that is a
    multiple of 4.  If applicable, padding will be applied to extend the length
    of the string input.

    Args:
        s (str): A string object.

    Returns:
        bytes: A bytes object.
    """

    r = len(s) % 4
    if r != 0:
        s += "=" * (4 - r)

    return s.encode("utf-8")


def _xor(bytes1: bytes, bytes2: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(bytes1, bytes2))


def _hmac(key: bytes, msg: bytes, hash: str) -> bytes:
    return hmac.new(key, msg, hash).digest()
