"""SCRAM Authentication Support for a Project Haystack Client

This module implements client-side logic and support for the following:
    1. Initiation of an authentication exchange according to Project Haystack
    2. SCRAM authentication according to RFC5802
"""

from __future__ import annotations

import hashlib
import hmac
import re
from base64 import b64encode, urlsafe_b64decode
from dataclasses import dataclass
from email.message import Message
from functools import cached_property
from hashlib import pbkdf2_hmac
from random import randbytes
from typing import TYPE_CHECKING
from urllib.error import HTTPError

from phable.http import ph_request
from phable.logger import log_http_req, log_http_res

if TYPE_CHECKING:
    from ssl import SSLContext


@dataclass
class AuthError(Exception):
    """Error raised when the client is unable to authenticate with the server using the
    credentials provided.

    `AuthError` can be directly imported as follows:

    ```python
    from phable import AuthError
    ```

    Parameters:
        help_msg: A display to help with troubleshooting.
    """

    help_msg: str


@dataclass
class ScramServerSignatureNotEqualError(Exception):
    help_msg: str


@dataclass
class ScramServerResponseParsingError(Exception):
    help_msg: str


class ScramScheme:
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        content_type: str,
        context: SSLContext | None = None,
    ):
        self.uri: str = uri[0:-1] if uri[-1] == "/" else uri
        self.username: str = username
        self._password: str = password
        self._content_type = content_type
        self._context = context

        # others to be defined later
        self._handshake_token: str
        self._hash: str
        self._s_nonce: str
        self._salt: str
        self._iter_count: int
        self._auth_token: str

    _client_nonce_bytes: int = 12
    _gs2_header: str = "n,,"

    # -------------------------------------------------------------------------
    # Send HTTP messages following scram to get auth token
    # -------------------------------------------------------------------------
    def get_auth_token(self) -> str:
        self._hello_call()
        self._first_call()
        self._final_call()

        return self._auth_token

    def _hello_call(self) -> None:
        """Defines and sends the HELLO message to the server and processes the
        server's response according to Project Haystack's SCRAM auth
        instructions.
        """

        headers = {
            "Authorization": f"HELLO username={_to_base64(self.username)}",
        }
        res_headers = self._ph_scram_get(
            self.uri + "/about",
            headers,
            context=self._context,
        )

        try:
            self._handshake_token, self._hash = _parse_hello_call_result(res_headers)
        except Exception:
            raise ScramServerResponseParsingError(
                "Unable to parse the server's response to the client's Hello call message"
            )

    def _first_call(self) -> None:
        """Defines and sends the "client-first-message" to the server and
        processes the server's response according to RFC5802."""

        c_nonce = randbytes(self._client_nonce_bytes).hex()
        self._c1_bare = f"n={self.username},r={c_nonce}"
        c1_msg = self._gs2_header + self._c1_bare

        headers = {
            "Authorization": f"SCRAM data={_to_base64(c1_msg)}, handshakeToken={self._handshake_token}"
        }

        res_headers = self._ph_scram_get(
            self.uri + "/about",
            headers,
            context=self._context,
        )

        try:
            (
                self._s_nonce,
                self._salt,
                self._iter_count,
            ) = _parse_first_call_result(res_headers)
        except Exception:
            raise ScramServerResponseParsingError(
                "Unable to parse the server's response to the client's First call message"
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
                f"SCRAM data={self._client_final_message()}, handshakeToken={self._handshake_token}"
            )
        }

        res_headers = self._ph_scram_get(
            self.uri + "/about",
            headers,
            context=self._context,
        )

        try:
            self._auth_token, server_signature = _parse_final_call_result(res_headers)
        except Exception:
            raise ScramServerResponseParsingError(
                "Unable to parse the server's response to the client's Final call message"
            )

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
            self._salt,
            self._iter_count,
            self._parsed_hash,
            self._password,
        )

    @property
    def _parsed_hash(self) -> str:
        if self._hash == "SHA-256":
            return "sha256"
        else:
            raise ValueError

    @property
    def _client_key(self) -> bytes:
        return _hmac(self._salted_password, b"Client Key", self._parsed_hash)

    @property
    def _stored_key(self) -> bytes:
        hashFunc = hashlib.new(self._parsed_hash)
        hashFunc.update(self._client_key)
        return hashFunc.digest()

    @property
    def _server_key(self) -> bytes:
        return _hmac(self._salted_password, b"Server Key", self._parsed_hash)

    @property
    def _server_signature(self) -> str:
        server_signature = _hmac(
            self._server_key,
            self._auth_message.encode("utf-8"),
            self._parsed_hash,
        )
        server_signature = b64encode(server_signature).decode("utf-8")
        return server_signature

    def _client_final_message(self) -> str:
        s1_msg = f"r={self._s_nonce},s={self._salt},i={self._iter_count}"

        client_final_no_proof = f"c={_to_base64('n,,')},r={self._s_nonce}"

        c_nonce = self._s_nonce[
            0 : (self._client_nonce_bytes * 2)
        ]  # 2 chars per bytes for hex
        c1_bare = f"n={self.username},r={c_nonce}"
        self._auth_message = f"{c1_bare},{s1_msg},{client_final_no_proof}"

        client_signature = _hmac(
            self._stored_key, self._auth_message.encode("utf-8"), self._parsed_hash
        )

        client_proof = _to_base64(_xor(self._client_key, client_signature))
        client_final = client_final_no_proof + ",p=" + client_proof

        return _to_base64(client_final)

    def _ph_scram_get(
        self,
        url: str,
        headers: dict[str, str],
        context: SSLContext | None = None,
    ) -> Message:
        try:
            response = ph_request(url, headers, self._content_type, context=context)
            res_headers = response.headers
        except HTTPError as e:
            res_headers = e.headers

            log_http_req("GET", url, headers)
            log_http_res(e.status, dict(res_headers))

            if e.status == 403:
                raise AuthError(
                    "Unable to authenticate with the server using the credentials "
                    + "provided."
                )

        return res_headers


def _parse_hello_call_result(
    hello_call_result_headers: Message,
) -> tuple[str, str]:
    """Parses the handshake token and hash from the 'WWW-Authenticate' header
    in the server generated HELLO message.
    """

    auth_header = hello_call_result_headers["WWW-Authenticate"]

    # find the handshake token
    exclude_msg = "handshakeToken="
    s = re.search(f"({exclude_msg})[a-zA-Z0-9+=/]+", auth_header)

    start_index = len(exclude_msg)
    handshake_token = s.group(0)[start_index:]

    # find the hash
    exclude_msg = "hash="
    s = re.search(f"({exclude_msg})(SHA-256)", auth_header)

    start_index = len(exclude_msg)
    hash = s.group(0)[start_index:]

    return handshake_token, hash


def _parse_first_call_result(
    first_call_result_headers: Message,
) -> tuple[str, str, int]:
    """Parses the server nonce, salt, and iteration count from the
    'WWW-Authenticate' header in the "server-first-message".
    """

    auth_header = first_call_result_headers["WWW-Authenticate"]

    exclude_msg = "data="
    scram_data = re.search(f"({exclude_msg})[a-zA-Z0-9+=/]+", auth_header)

    start_index = len(exclude_msg)
    decoded_scram_data = _from_base64(scram_data.group(0)[start_index:])
    s_nonce, salt, iteration_count = decoded_scram_data.replace(" ", "").split(",")

    return (
        s_nonce.replace("r=", "", 1),
        salt.replace("s=", "", 1),
        int(iteration_count.replace("i=", "", 1)),
    )


def _parse_final_call_result(
    final_call_result_headers: Message,
) -> tuple[str, str]:
    """Parses the auth token from the 'WWW-Authenticate' header in the
    "server-final-message".
    """
    auth_header = str(final_call_result_headers)

    exclude_msg1 = "authToken="
    s1 = re.search(f"({exclude_msg1})[^,]+", auth_header)

    start_index = len(exclude_msg1)
    auth_token = s1.group(0)[start_index:]

    exclude_msg2 = "data="
    s2 = re.search(f"({exclude_msg2})[a-zA-Z0-9+=/]+", auth_header)

    start_index = len(exclude_msg2)
    data = s2.group(0)[start_index:]

    server_signature = _from_base64(data).replace("v=", "", 1)

    return auth_token, server_signature


# -----------------------------------------------------------------------------
# Util functions for computations
# -----------------------------------------------------------------------------


def _to_base64(msg: str | bytes) -> str:
    """Perform base64uri encoding of a message as defined by RFC 4648."""

    # Convert str inputs to bytes
    if isinstance(msg, str):
        msg = msg.encode("utf-8")

    output = b64encode(msg)

    # Decode the output as a str
    output = output.decode("utf-8")

    # Remove padding
    output = output.replace("=", "")

    return output


def _salted_password(
    salt: str, iterations: int, hash_func: str, password: str
) -> bytes:
    """Generates a salted password according to RFC5802."""
    key_bytes = int(_key_bits(hash_func) / 8)
    dk = pbkdf2_hmac(
        hash_func, password.encode(), urlsafe_b64decode(salt), iterations, key_bytes
    )

    return dk


def _key_bits(hash_func: str) -> int:
    match hash_func:
        case "sha256":
            return 256
        case _:
            raise ValueError(f"Unsupported hash function: {hash_func}")


def _from_base64(msg: str) -> str:
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
