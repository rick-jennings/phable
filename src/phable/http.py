from __future__ import annotations

import importlib.metadata
import ssl
import urllib.request
from dataclasses import dataclass
from email.message import Message
from http.client import HTTPResponse
from typing import TYPE_CHECKING, Any
from urllib.error import URLError

from phable.kinds import Grid
from phable.logger import log_http_req, log_http_res

if TYPE_CHECKING:
    from ssl import SSLContext


@dataclass
class PhHttpResponse:
    """HTTP response container for data read from stream buffer data and more.

    Includes utility method to convert body to a Grid.
    """

    body: bytes
    headers: Message
    status: int


def ph_request(
    url: str,
    headers: dict[str, Any],
    content_type: str,
    data: bytes | None = None,
    method: str = "GET",
    context: SSLContext | None = None,
) -> PhHttpResponse:
    headers = headers.copy()
    headers["Content-Type"] = content_type

    http_response = request(
        url,
        headers,
        data,
        method,
        context,
    )
    ph_res = PhHttpResponse(
        headers=http_response.headers,
        status=http_response.status,
        body=http_response.read(),
    )

    http_response.close()

    log_http_res(ph_res.status, dict(ph_res.headers), ph_res.body)

    return ph_res


# only use request if a BufferedReader is required, otherwise use ph_request()
def request(
    url: str,
    headers: dict[str, Any],
    data: bytes | Grid | None = None,
    method: str = "GET",
    context: SSLContext | None = None,
) -> HTTPResponse:
    if not url.startswith("http"):
        raise URLError('URL must begin with the prefix "http"')

    headers = headers.copy()
    headers["User-Agent"] = f"phable/{importlib.metadata.version('phable')}"

    httprequest = urllib.request.Request(url, data=data, headers=headers, method=method)  # ty: ignore [invalid-argument-type]

    if context is None:
        context = ssl.create_default_context()

    http_res = urllib.request.urlopen(httprequest, context=context)

    log_http_req(
        httprequest.get_method(),
        httprequest.full_url,
        httprequest.headers,  # ty: ignore [invalid-argument-type]
        httprequest.data,  # ty: ignore [invalid-argument-type]
    )

    # do not log the http res data since the buffer can be read only once!
    # http res data is logged in ph_request()

    return http_res
