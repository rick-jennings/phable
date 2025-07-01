from __future__ import annotations

import importlib.metadata
import json
import ssl
import urllib.request
from dataclasses import dataclass
from email.message import Message
from http.client import HTTPResponse
from typing import TYPE_CHECKING, Any
from urllib.error import URLError

from phable.kinds import Grid
from phable.logger import log_http_req, log_http_res
from phable.parsers.json import grid_to_json, json_to_grid

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

    def to_grid(self) -> dict[str, Any]:
        return json_to_grid(json.loads(self.body.decode("utf-8")))


def ph_request(
    url: str,
    headers: dict[str, Any],
    data: Grid | None = None,
    method: str = "GET",
    context: SSLContext | None = None,
) -> PhHttpResponse:
    headers = headers.copy()

    # call reqs use Grid for data & scram reqs use None
    if isinstance(data, Grid):
        data = grid_to_json(data)
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"

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

    if isinstance(data, Grid):
        data = grid_to_json(data)

    headers = headers.copy()
    headers["User-Agent"] = f"phable/{importlib.metadata.version('phable')}"

    httprequest = urllib.request.Request(url, data=data, headers=headers, method=method)

    if context is None:
        context = ssl.create_default_context()

    http_res = urllib.request.urlopen(httprequest, context=context)

    log_http_req(
        httprequest.get_method(),
        httprequest.full_url,
        httprequest.headers,
        httprequest.data,
    )

    # do not log the http res data since the buffer can be read only once!
    # http res data is logged in ph_request()

    return http_res
