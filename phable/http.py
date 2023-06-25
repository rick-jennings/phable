import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from email.message import Message
from typing import Any, Optional

from phable.kinds import Grid
from phable.parser.json import json_to_grid

logger = logging.getLogger(__name__)


@dataclass
class HttpResponse:
    body: str
    headers: Message
    status: int
    error_count: int = 0

    @property
    def grid(self) -> Grid:
        return json_to_grid(json.loads(self.body))


def request(
    url: str,
    data: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, Any]] = None,
    method: str = "GET",
    error_count: int = 0,
) -> HttpResponse:
    """Performs an HTTP request."""
    if not url.startswith("http"):
        raise urllib.error.URLError(
            "Incorrect and possibly insecure protocol in url"
        )
    headers = headers or {}
    data = data or {}

    request_data = json.dumps(data).encode()
    headers["Content-Type"] = "application/json; charset=UTF-8"

    httprequest = urllib.request.Request(
        url, data=request_data, headers=headers, method=method
    )

    try:
        with urllib.request.urlopen(
            httprequest, context=ssl.create_default_context()
        ) as httpresponse:
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

    return response
