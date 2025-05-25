import importlib.metadata
import json
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from email.message import Message
from typing import Any

from phable.kinds import Grid
from phable.parsers.json import grid_to_json, json_to_grid


@dataclass
class IncorrectHttpResponseStatus(Exception):
    help_msg: str
    actual_status: int


@dataclass
class PhHttpResponse:
    body: str
    headers: Message
    status: int

    def to_grid(self) -> dict[str, Any]:
        return json_to_grid(json.loads(self.body.decode("utf-8")))


def ph_post(
    url: str,
    data: dict[str, Any] | bytes | Grid | None = None,
    headers: dict[str, str] | None = None,
    context=None,
) -> PhHttpResponse:
    response = _ph_request(
        url,
        data=data,
        headers=headers,
        method="POST",
        context=context,
    )

    # all POST requests must return a 200 HTTP code to be successful.
    # if this ever changes then might want to move this logic to
    # HaxallClient.call()
    if response.status != 200:
        raise IncorrectHttpResponseStatus(
            f"Expected status 200 and received status {response.status}.",
            response.status,
        )

    return response


def ph_get(
    url: str,
    data: dict[str, Any] | bytes | Grid | None = None,
    headers: dict[str, str] | None = None,
    context=None,
) -> PhHttpResponse:
    return _ph_request(
        url,
        data=data,
        headers=headers,
        method="GET",
        context=context,
    )


def _ph_request(
    url: str,
    data: dict[str, Any] | bytes | Grid | None = None,
    headers: dict[str, Any] | None = None,
    method: str = "GET",
    error_count: int = 0,
    context=None,
) -> PhHttpResponse:
    if not url.startswith("http"):
        raise urllib.error.URLError('URL must begin with the prefix "http"')

    headers = headers or {}
    data = data or {}

    if isinstance(data, Grid):
        data = grid_to_json(data)

    if headers.get("Content-Type") is None:
        request_data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json; charset=UTF-8"
    else:
        request_data = data

    headers["User-Agent"] = f"phable/{importlib.metadata.version('phable')}"

    httprequest = urllib.request.Request(
        url, data=request_data, headers=headers, method=method
    )

    if context is None:
        context = ssl.create_default_context()

    try:
        with urllib.request.urlopen(httprequest, context=context) as httpresponse:
            response = PhHttpResponse(
                headers=httpresponse.headers,
                status=httpresponse.status,
                body=httpresponse.read(),
            )
    except urllib.error.HTTPError as e:
        response = PhHttpResponse(
            body=str(e.reason),
            headers=e.headers,
            status=e.code,
        )

    return response
