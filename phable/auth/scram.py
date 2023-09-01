"""SCRAM Authentication Support for a Project Haystack Client

This module implements client-side logic and support for the following:
    1. Initiation of an authentication exchange according to Project Haystack
    2. SCRAM authentication according to RFC5802
"""

import hashlib
import hmac
import re
from base64 import b64encode, urlsafe_b64decode, urlsafe_b64encode
from dataclasses import dataclass
from functools import cached_property
from hashlib import pbkdf2_hmac
from typing import Callable
from uuid import uuid4

# -----------------------------------------------------------------------------
# Use dependency inversion so we don't need to define the HTTP client code here
# -----------------------------------------------------------------------------
HttpGetHeaders = Callable[[str, dict[str, str]], dict[str, str]]
# Note: We expect parameters for HttpGetHeaders to include uri as a str and
# headers as a dict.  Headers are expected to be returned as a dict.


# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------


@dataclass
class ScramServerSignatureNotEqualError(Exception):
    help_msg: str


@dataclass
class ScramAuthError(Exception):
    help_msg: str


@dataclass
class ScramRespParsingError(Exception):
    help_msg: str


# -----------------------------------------------------------------------------
# Scram scheme core interface
# -----------------------------------------------------------------------------


class ScramScheme:
    def __init__(
        self, http_get: HttpGetHeaders, uri: str, username: str, password: str
    ):
        self.http_get: HttpGetHeaders = http_get
        self.uri: str = uri
        self.username: str = username
        self._password: str = password

        # others to be defined later
        self._handshake_token: str
        self._hash: str
        self._c1_bare: str
        self._s_nonce: str
        self._salt: str
        self._iter_count: int
        self._auth_token: str

    # -------------------------------------------------------------------------
    # Send HTTP messages following scram to get auth token
    # -------------------------------------------------------------------------
    def get_auth_token(self) -> str:
        try:
            self._hello_call()
            self._c1_bare = _c1_bare(self.username)
            self._first_call()
            self._final_call()
        except Exception:
            raise ScramAuthError(
                "Unable to authenticate with the server using scram"
            )

        return self._auth_token

    def _hello_call(self) -> None:
        """Defines and sends the HELLO message to the server and processes the
        server's response according to Project Haystack's SCRAM auth
        instructions.
        """

        headers = {
            "Authorization": f"HELLO username={_to_base64(self.username)}"
        }
        response = self.http_get(self.uri + "/about", headers)

        self._handshake_token, self._hash = _parse_hello_call_result(response)

    def _first_call(self) -> None:
        """Defines and sends the "client-first-message" to the server and
        processes the server's response according to RFC5802."""

        gs2_header = "n,,"
        headers = {
            "Authorization": f"scram handshakeToken={self._handshake_token}, "
            f"hash={self._hash}, data={_to_base64(gs2_header+self._c1_bare)}"
        }
        response = self.http_get(self.uri + "/about", headers)

        self._s_nonce, self._salt, self._iter_count = _parse_first_call_result(
            response
        )

    def _final_call(self) -> None:
        """Defines and sends the "client-final-message" to the server and
        processes the server's response according to RFC5802.

        If the SCRAM authentication exchange was successful then the auth
        token parsed from the server's response is assigned to the _auth_token
        attribute in this class, which may be used in future requests to the
        server.

        Raises a ServerSignatureNotEqualError if the client's computed
        ServerSignature does not match the one received by the server.
        """

        headers = {
            "Authorization": (
                f"scram handshaketoken={self._handshake_token},"
                f"data={self._client_final_message}"
            )
        }

        response = self.http_get(self.uri + "/about", headers)

        self._auth_token, server_signature = _parse_final_call_result(response)

        if server_signature != self._server_signature:
            raise ScramServerSignatureNotEqualError(
                "Raised when the ServerSignature value sent by the server "
                "does not equal the ServerSignature computed by the client."
            )

    # -------------------------------------------------------------------------
    # Use class properties for calculations since there is so much state
    # -------------------------------------------------------------------------

    @cached_property
    def _salted_password(self) -> bytes:
        return _salted_password(
            self._salt, self._iter_count, self._hash, self._password
        )

    @property
    def _client_key(self) -> bytes:
        return _hmac(self._salted_password, b"Client Key", self._hash)

    @property
    def _stored_key(self) -> bytes:
        hashFunc = hashlib.new(self._hash)
        hashFunc.update(self._client_key)
        return hashFunc.digest()

    @property
    def _client_final_no_proof(self) -> str:
        return f"c={_to_base64('n,,')},r={self._s_nonce}"

    @property
    def _auth_message(self) -> str:
        return (
            f"{self._c1_bare},r={self._s_nonce},s={self._salt},"
            + f"i={self._iter_count},{self._client_final_no_proof}"
        )

    @property
    def _client_signature(self) -> bytes:
        return _hmac(
            self._stored_key, self._auth_message.encode("utf-8"), self._hash
        )

    @property
    def _client_proof(self) -> str:
        return _to_base64(_xor(self._client_key, self._client_signature))

    @property
    def _server_key(self) -> bytes:
        return _hmac(self._salted_password, b"Server Key", self._hash)

    @property
    def _server_signature(self) -> str:
        server_signature = _hmac(
            self._server_key, self._auth_message.encode("utf-8"), self._hash
        )
        server_signature = b64encode(server_signature).decode("utf-8")
        return server_signature

    @property
    def _client_final_message(self) -> str:
        client_final = self._client_final_no_proof + ",p=" + self._client_proof
        return _to_base64(client_final)


# -----------------------------------------------------------------------------
# Util functions for parsing HTTP responses
# -----------------------------------------------------------------------------


def _parse_hello_call_result(
    hello_call_result_headers: dict[str, str],
) -> tuple[str, str]:
    """Parses the handshake token and hash from the 'WWW-Authenticate' header
    in the server generated HELLO message.
    """

    auth_header = hello_call_result_headers["WWW-Authenticate"]

    # find the handshake token
    exclude_msg = "handshakeToken="
    s = re.search(f"({exclude_msg})[a-zA-Z0-9]+", auth_header)

    if s is None:
        raise ScramRespParsingError(
            (
                "Handshake token not found in the 'WWW-Authenticate' header:"
                + f"\n{auth_header}"
            )
        )

    start_index = len(exclude_msg)
    handshake_token = s.group(0)[start_index:]

    # find the hash
    exclude_msg = "hash="
    s = re.search(f"({exclude_msg})(SHA-256)", auth_header)

    if s is None:
        raise ScramRespParsingError(
            "Hash method not found in the 'WWW-Authenticate' header:"
            f"\n{auth_header}"
        )

    start_index = len(exclude_msg)
    s_new = s.group(0)[start_index:]

    if s_new == "SHA-256":
        s_new = "sha256"

    hash = s_new

    return handshake_token, hash


def _parse_first_call_result(
    first_call_result_headers: dict[str, str],
) -> tuple[str, str, int]:
    """Parses the server nonce, salt, and iteration count from the
    'WWW-Authenticate' header in the "server-first-message".
    """

    auth_header = first_call_result_headers["WWW-Authenticate"]
    exclude_msg = "data="
    scram_data = re.search(f"({exclude_msg})[a-zA-Z0-9]+", auth_header)

    if scram_data is None:
        raise ScramRespParsingError(
            "Scram data not found in the 'WWW-Authenticate' header:"
            f"\n{auth_header}"
        )

    start_index = len(exclude_msg)
    decoded_scram_data = _from_base64(scram_data.group(0)[start_index:])
    s_nonce, salt, iteration_count = decoded_scram_data.replace(" ", "").split(
        ","
    )

    if "r=" not in s_nonce:
        raise ScramRespParsingError(
            "Server nonce not found in the 'WWW-Authenticate' header:"
            f"\n{auth_header}"
        )
    elif "s=" not in salt:
        raise ScramRespParsingError(
            f"Salt not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )
    elif "i=" not in iteration_count:
        raise ScramRespParsingError(
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


def _parse_final_call_result(
    final_call_result_headers: dict[str, str]
) -> tuple[str, str]:
    """Parses the auth token from the 'WWW-Authenticate' header in the
    "server-final-message".
    """

    auth_header = final_call_result_headers.as_string()

    exclude_msg1 = "authToken="
    s1 = re.search(f"({exclude_msg1})[^,]+", auth_header)

    if s1 is None:
        raise ScramRespParsingError(
            "Auth token not found in the 'WWW-Authenticate' header:"
            f"\n{auth_header}"
        )

    start_index = len(exclude_msg1)
    auth_token = s1.group(0)[start_index:]

    exclude_msg2 = "data="
    s2 = re.search(f"({exclude_msg2})[^,]+", auth_header)

    start_index = len(exclude_msg2)
    data = s2.group(0)[start_index:]

    server_signature = _from_base64(data).replace("v=", "", 1)

    return auth_token, server_signature


# -----------------------------------------------------------------------------
# Util functions for computations
# -----------------------------------------------------------------------------


def _c1_bare(username: str) -> str:
    nonce = str(uuid4()).replace("-", "")
    return f"n={username},r={nonce}"


def _to_base64(msg: str | bytes) -> str:
    """Perform base64uri encoding of a message as defined by RFC 4648."""

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
    dk = pbkdf2_hmac(
        hash_func, password.encode(), urlsafe_b64decode(salt), iterations
    )
    return dk


def _from_base64(msg: str) -> str:  # str
    return urlsafe_b64decode(_to_bytes(msg)).decode("utf-8")


def _to_bytes(s: str) -> bytes:
    """Convert a string to a bytes object.

    Prior to conversion to bytes the string object must have a length that is a
    multiple of 4.  If applicable, padding will be applied to extend the length
    of the string input.
    """

    r = len(s) % 4
    if r != 0:
        s += "=" * (4 - r)

    return s.encode("utf-8")


def _xor(bytes1: bytes, bytes2: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(bytes1, bytes2))


def _hmac(key: bytes, msg: bytes, hash: str) -> bytes:
    return hmac.new(key, msg, hash).digest()
