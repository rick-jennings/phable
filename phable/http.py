import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from email.message import Message
from typing import Any, Optional

from phable.kinds import Grid
from phable.parsers.json import grid_to_json, json_to_grid

# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------


@dataclass
class IncorrectHttpResponseStatus(Exception):
    help_msg: str
    actual_status: int


# -----------------------------------------------------------------------------
# Data model for HTTP response
# -----------------------------------------------------------------------------


@dataclass
class HttpResponse:
    body: str
    headers: Message
    status: int
    error_count: int = 0

    def to_grid(self) -> dict[str, Any]:
        return json_to_grid(json.loads(self.body))


# -----------------------------------------------------------------------------
# Post and get header funcs tailored for Project Haystack
# -----------------------------------------------------------------------------


def post(
    url: str, post_data: Grid, headers: dict[str, str], context=None
) -> dict[str, Any]:
    # if post_data is None:
    #     post_data = {}
    response = request(
        url, data=post_data, headers=headers, method="POST", context=context
    )

    if response.status != 200:
        raise IncorrectHttpResponseStatus(
            f"Expected status 200 and received status {response.status}.",
            response.status,
        )

    return response.to_grid()


# -----------------------------------------------------------------------------
# Foundation for all requests
# -----------------------------------------------------------------------------


def request(
    url: str,
    data: Grid | None = None,
    headers: Optional[dict[str, Any]] = None,
    method: str = "GET",
    error_count: int = 0,
    context=None,
) -> HttpResponse:
    """Performs an HTTP request."""
    if not url.startswith("http"):
        raise urllib.error.URLError("Incorrect and possibly insecure protocol in url")
    headers = headers or {}
    data = data or {}

    if isinstance(data, Grid):
        data = grid_to_json(data)

    request_data = json.dumps(data).encode()
    headers["Content-Type"] = "application/json; charset=UTF-8"

    httprequest = urllib.request.Request(
        url, data=request_data, headers=headers, method=method
    )

    if context is None:
        context = ssl.create_default_context()

    try:
        with urllib.request.urlopen(httprequest, context=context) as httpresponse:
            response = HttpResponse(
                headers=httpresponse.headers,
                status=httpresponse.status,
                body=httpresponse.read().decode(
                    httpresponse.headers.get_content_charset("utf-8")
                ),
            )
    except urllib.error.HTTPError as e:
        response = HttpResponse(
            body=str(e.reason),
            headers=e.headers,
            status=e.code,
            error_count=error_count + 1,
        )

    if response.status == 403:
        raise IncorrectHttpResponseStatus("", response.status)

    return response
