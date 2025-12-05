import json
import logging
from typing import Any

logger = logging.getLogger("phable")


def log_http_req(
    method: str, url: str, headers: dict[str, Any], data: bytes | None = None
) -> None:
    logger.debug(
        "req >\n\n" + method + " " + url + "\n\n" + _get_http_log(headers, data)
    )


def log_http_res(
    status: int, headers: dict[str, Any], data: bytes | None = None
) -> None:
    logger.debug(
        "res <\n\n" + _get_status_text(status) + "\n\n" + _get_http_log(headers, data)
    )


def _get_http_log(headers: dict[str, str], data: bytes | None) -> str:
    if data is None:
        data = ""
    elif headers.get("Content-Type") == "application/json":
        data = _pretty_json_print(data)
    else:
        data = data.decode("utf-8", errors="ignore")

    return "Headers:\n" + json.dumps(headers, indent=2) + "\n\nData:\n" + data + "\n"


def _pretty_json_print(data: bytes) -> str:
    data = json.loads(data)
    return json.dumps(data, indent=2)


def _get_status_text(status: int) -> str:
    if status == 200:
        return "200 OK"
    else:
        return str(status)
