"""SCRAM Authentication Support for a Project Haystack Client

This module implements client-side logic and support for the following:
    1. Initiation of an authentication exchange according to Project Haystack
    2. SCRAM authentication according to RFC5802

Note:   HTTP messages are not sent by code within this module.  Functions defined in
        this module are used in HTTP messages sent and received by the Client class in
        `phable.client.py`.
"""


import hashlib
import hmac
import logging
import re
from base64 import urlsafe_b64decode, urlsafe_b64encode, b64encode
from binascii import hexlify, unhexlify
from dataclasses import dataclass
from hashlib import pbkdf2_hmac
from random import getrandbits
from time import time_ns
from typing import Any
from phable.exceptions import NotFoundError, ServerSignatureNotEqualError

from phable.http import HttpResponse

logger = logging.getLogger(__name__)


# TODO: Check the validity of the server final message


@dataclass
class HelloCallResult:
    handshake_token: str
    hash: str


@dataclass
class FirstCallResult:
    s_nonce: str
    salt: str
    iter_count: int


def hello_call_headers(username: str) -> dict[str, str]:
    """Defines the headers for the client generated HELLO message according to Project
    Haystack's SCRAM auth instructions."""
    username = _to_base64(username)
    headers = {"Authorization": f"HELLO username={username}"}
    return headers


def parse_hello_result(resp: HttpResponse) -> HelloCallResult:
    """Parses the server generated HELLO message according to Project Haystack's SCRAM
    auth instructions."""
    auth_header = resp.headers["WWW-Authenticate"]
    handshake_token = _parse_handshake_token(auth_header)
    hash = _parse_hash_func(auth_header)
    return HelloCallResult(handshake_token, hash)


def first_call_headers(
    hello_call_result: HelloCallResult, c1_bare: str
) -> dict[str, Any]:
    """Defines the headers for the "client-first-message" according to RFC5802."""
    handshake_token = hello_call_result.handshake_token
    hash = hello_call_result.hash

    gs2_header = "n,,"

    headers = {
        "Authorization": f"scram handshakeToken={handshake_token}, "
        f"hash={hash}, data={_to_base64(gs2_header+c1_bare)}"
    }

    return headers


def parse_first_result(first_result: HttpResponse) -> FirstCallResult:
    """Parses the "server-first-message" according to RFC5802."""
    auth_header = first_result.headers["WWW-Authenticate"]
    r, s, i = _parse_scram_data(auth_header)
    return FirstCallResult(r, s, i)


def final_call_headers(
    password: str,
    hello_call_result: HelloCallResult,
    c1_bare: str,
    first_call_result: FirstCallResult,
) -> tuple[str, dict[str, Any]]:
    """Defines the headers for the "client-final-message" according to RFC5802."""
    hash = hello_call_result.hash
    handshake_token = hello_call_result.handshake_token

    s_nonce = first_call_result.s_nonce
    salt = first_call_result.salt
    iter_count = first_call_result.iter_count

    # define the client final no proof
    client_final_no_proof = f"c={_to_base64('n,,')},r={s_nonce}"

    # define the auth msg
    auth_msg = (
        f"{c1_bare},r={s_nonce},s={salt}," + f"i={iter_count},{client_final_no_proof}"
    )

    # define the salted password
    salted_password = _salted_password(
        salt,
        iter_count,
        hash,
        password,
    )

    # define the client key
    client_key = _hmac(salted_password, b"Client Key", hash)

    # define the server key
    server_key = _hmac(salted_password, b"Server Key", hash)

    # define the server signature
    server_signature = _hmac(server_key, auth_msg.encode("utf-8"), hash)
    server_signature = b64encode(server_signature).decode("utf-8")
    logger.critical(f"Here is the base64 encoded server signature:\n{server_signature}")

    # define the stored key
    hashFunc = hashlib.new(hash)
    hashFunc.update(client_key)
    stored_key: bytes = hashFunc.digest()

    # find the client signature
    client_signature = _hmac(stored_key, auth_msg.encode("utf-8"), hash)

    # find the client proof
    client_proof = xor(client_key, client_signature)

    client_proof_encode = _to_base64(client_proof)

    client_final = client_final_no_proof + ",p=" + client_proof_encode
    client_final_base64 = _to_base64(client_final)

    final_msg = f"scram handshaketoken={handshake_token},data={client_final_base64}"

    headers = {"Authorization": final_msg}
    return (server_signature, headers)


def xor(bytes1: bytes, bytes2: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(bytes1, bytes2))


def _hmac(key: bytes, msg: bytes, hash: str) -> bytes:
    return hmac.new(key, msg, hash).digest()


def parse_final_result(resp: HttpResponse, server_signature: str) -> str:
    """Parses the auth token from the "server-final-message" and authenticates the
    server according to RFC5802."""
    auth_header = resp.headers.as_string()

    logger.critical(f"Here is the auth header:\n{auth_header}")

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
    logger.critical(f"Here is the data:\n{data}")

    server_signature_from_server = _from_base64(data).replace("v=", "")

    if server_signature != server_signature_from_server:
        raise ServerSignatureNotEqualError

    return auth_token


def _parse_scram_data(auth_header: str) -> tuple[str, str, int]:
    """Parses and decodes scram data from the contents of a 'WWW-Authenticate' header.

    Args:
        auth_header (str): Contents of the 'WWW-Authenticate' header in the HTTP
        response received from the server.  Search 'WWW-Authenticate' in the Project
        Haystack docs for more details.

    Raises:
        NotFoundError: When the parameter, auth_header, or its decoded variant does not
        contain one or more expected substrings.

    Returns:
        tuple[str, str, int]: Index 0 is the server nonce, Index 1 is the salt, and
        Index 2 is the iteration count which are used by the client for SCRAM
        authentication.
    """

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
        s_nonce.replace("r=", ""),
        salt.replace("s=", ""),
        int(iteration_count.replace("i=", "")),
    )


def _parse_handshake_token(auth_header: str) -> str:
    """Parses the handshake token from the contents of a 'WWW-Authenticate' header.

    Args:
        auth_header (str): Contents of the 'WWW-Authenticate' header in the HTTP
        response received from the server.  Search 'WWW-Authenticate' in the Project
        Haystack docs for more details.

    Raises:
        NotFoundError: When the parameter, auth_header, does not contain the expected
        substring.

    Returns:
        str: Handshake token defined by the server which the client is required to use
        for SCRAM authentication.
    """

    exclude_msg = "handshakeToken="
    s = re.search(f"({exclude_msg})[a-zA-Z0-9]+", auth_header)

    if s is None:
        raise NotFoundError(
            (
                "Handshake token not found in the 'WWW-Authenticate' header:"
                + f"\n{auth_header}"
            )
        )

    return s.group(0)[len(exclude_msg) :]


def _parse_hash_func(auth_header: str) -> str:
    """Parses the hash function from the contents of a 'WWW-Authenticate' header.

    Args:
        auth_header (str): Contents of the 'WWW-Authenticate' header in the HTTP
        response received from the server.  Search 'WWW-Authenticate' in the Project
        Haystack docs for more details.

    Raises:
        NotFoundError: When the parameter, auth_header, does not contain the expected
        substring.

    Returns:
        str: Cryptographic hash function defined by the server which the client is
        required to use for SCRAM authentication.
    """

    exclude_msg = "hash="
    s = re.search(f"({exclude_msg})(SHA-256)", auth_header)

    if s is None:
        raise NotFoundError(
            f"Hash method not found in the 'WWW-Authenticate' header:\n{auth_header}"
        )

    s_new = s.group(0)[len(exclude_msg) :]

    if s_new == "SHA-256":
        s_new = "sha256"

    return s_new


def _to_custom_hex(x: int, length: int) -> str:
    """Convert an integer x to hexadecimal string representation without a prepended
    '0x' str.  Prepend leading zeros as needed to ensure the specified number of nibble
    characters.
    """

    # Convert x to a hexadecimal number
    x_hex = hex(x)

    # Remove prepended 0x used to describe hex numbers
    x_hex = x_hex.replace("0x", "")

    # Prepend 0s as needed
    if len(x_hex) < length:
        x_hex = "0" * (length - len(x_hex)) + x_hex

    return x_hex


def gen_nonce() -> str:
    """Generate a nonce."""
    # Notes:
    #   getrandbits() defines a random 64 bit integer
    #   time_ns() defines ticks since the Unix epoch (1 January 1970)

    # Define nonce random mask for this VM
    nonce_mask: int = getrandbits(64)

    rand = getrandbits(64)
    ticks = time_ns() ^ nonce_mask ^ rand
    return _to_custom_hex(rand, 16) + _to_custom_hex(ticks, 16)


def _salted_password(
    salt: str, iterations: int, hash_func: str, password: str
) -> bytes:
    """Generates a salted password according to RFC5802."""
    # Need hash_func to be a str here
    dk = pbkdf2_hmac(hash_func, password.encode(), urlsafe_b64decode(salt), iterations)
    # encrypt_password = hexlify(dk)
    # return encrypt_password
    return dk


def _to_base64(msg: str | bytes) -> str:
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


def _from_base64(msg: str) -> str:
    """Decode a base64uri encoded message defined by RFC 4648 into
    its binary contents. Decode a URI-safe RFC 4648 encoding.

    Args:
        msg (str): A base64uri message to be decoded.

    Returns:
        str: A decoded message
    """

    # Decode base64uri
    decoded_msg = urlsafe_b64decode(_to_bytes(msg))

    # Decode bytes obj as a str
    return decoded_msg.decode("utf-8")


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
