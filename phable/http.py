import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from email.message import Message
from typing import Any, NamedTuple, Optional

from phable.kinds import Grid
from phable.parser.json import json_to_grid

logger = logging.getLogger(__name__)

# TODO:  Rethink except grid when json.JSONDecodeError
# TODO:  Can we simplify this?  Maybe get rid of http module altogether?


@dataclass
class InvalidCloseError(Exception):
    help_msg: str


class Response(NamedTuple):
    """Container for HTTP response."""

    body: str
    headers: Message
    status: int
    error_count: int = 0

    def to_grid(self) -> Grid:
        """
        Decode body's JSON.
        Returns:
            Haystack Grid
        """
        output = json_to_grid(json.loads(self.body))
        # TODO:  Raise a json to grid parsing error if parsing fails

        # try:
        #     output = json_to_grid(json.loads(self.body))
        # # TODO:  verify this code below is valid
        # except json.JSONDecodeError:
        #     output = Grid(
        #         meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[]
        #     )  # Previously was: ""
        return output


def request(
    url: str,
    data: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, Any]] = None,
    method: str = "GET",
    data_as_json: bool = True,
    error_count: int = 0,
) -> Response:
    """
    Perform HTTP request.
    Args:
        url: url to fetch
        data: dict of keys/values to be encoded and submitted
        params: dict of keys/values to be encoded in URL query string
        headers: optional dict of request headers
        method: HTTP method , such as GET or POST
        data_as_json: if True, data will be JSON-encoded
        error_count: optional current count of HTTP errors, to manage recursion
    Raises:
        URLError: if url starts with anything other than "http"
    Returns:
        A dict with headers, body, status code, and, if applicable, object
        rendered from JSON
    """
    if not url.startswith("http"):
        raise urllib.error.URLError("Incorrect and possibly insecure protocol in url")
    method = method.upper()
    request_data = None
    headers = headers or {}
    data = data or {}
    params = params or {}
    headers = {"Accept": "application/json", **headers}

    if method == "GET":
        params = {**params, **data}
        data = None

    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True, safe="/")

    if data:
        if data_as_json:
            request_data = json.dumps(data).encode()
            headers["Content-Type"] = "application/json; charset=UTF-8"
            # logger.critical(f"Here is the request data:\n{request_data}")
        else:
            request_data = urllib.parse.urlencode(data).encode()

    httprequest = urllib.request.Request(
        url, data=request_data, headers=headers, method=method
    )

    try:
        with urllib.request.urlopen(
            httprequest, context=ssl.create_default_context()
        ) as httpresponse:
            response = Response(
                headers=httpresponse.headers,
                status=httpresponse.status,
                body=httpresponse.read().decode(
                    httpresponse.headers.get_content_charset("utf-8")
                ),
            )
    except urllib.error.HTTPError as e:
        response = Response(
            body=str(e.reason),
            headers=e.headers,
            status=e.code,
            error_count=error_count + 1,
        )

    return response  # .json()
