import logging
import socket
from urllib.error import URLError
from urllib.request import getproxies

import pytest

from phable.client.haxall import open_haxall_client


def test__log_proxy_settings_on_auth_url_error(
    URI: str,
    USERNAME: str,
    PASSWORD: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    proxy_variables = (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    )
    for variable in proxy_variables:
        monkeypatch.delenv(variable, raising=False)

    monkeypatch.setenv("no_proxy", "*")
    assert "http" not in getproxies()

    with open_haxall_client(URI, USERNAME, PASSWORD) as client:
        assert client.about()["vendorName"] == "SkyFoundry"

    caplog.clear()
    with socket.socket() as unavailable_proxy:
        unavailable_proxy.bind(("127.0.0.1", 0))
        proxy_port = unavailable_proxy.getsockname()[1]
        proxy_url = f"http://proxy-user:proxy-password@127.0.0.1:{proxy_port}/"

        monkeypatch.delenv("no_proxy")
        monkeypatch.setenv("http_proxy", proxy_url)
        assert getproxies().get("http") == proxy_url

        with caplog.at_level(logging.DEBUG, logger="phable"):
            with pytest.raises(URLError) as exc_info:
                with open_haxall_client(URI, USERNAME, PASSWORD):
                    pass

    notes = "\n".join(getattr(exc_info.value, "__notes__", []))
    safe_proxy_address = f"127.0.0.1:{proxy_port}"

    assert isinstance(exc_info.value.reason, OSError)
    assert "Proxy configuration detected" in notes
    assert safe_proxy_address in notes
    assert "proxy-user" not in notes
    assert "proxy-password" not in notes
    assert safe_proxy_address in caplog.text
    assert "proxy-user" not in caplog.text
    assert "proxy-password" not in caplog.text
