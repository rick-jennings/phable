import logging
from typing import Optional

from phable.http import request
from phable.kinds import Grid, Ref
from phable.auth.scram import (
    HelloCallResult,
    FirstCallResult,
    first_call_headers,
    hello_call_headers,
    last_call_headers,
    parse_hello_result,
    parse_first_result,
    parse_last_result,
    gen_nonce,
)

from phable.exceptions import InvalidCloseError, IncorrectHttpStatus

logger = logging.getLogger(__name__)


class Client:
    """
    A client interface to a Haystack Server used for authentication and Haystack ops.
    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        format: str = "application/json",
    ):
        self.uri: str = uri
        self.username: str = username
        self._password: str = password
        self.format: str = format

    # ----------------------------------------------------------------------------------
    # execute the scram auth scheme to get a valid auth token from the server
    # ----------------------------------------------------------------------------------

    def open(self) -> None:
        try:
            hello_result = self._hello_call()
            c1_bare = f"n={self.username},r={gen_nonce()}"
            first_result = self._first_call(hello_result, c1_bare)
            self._auth_token = self._last_call(hello_result, c1_bare, first_result)
        except Exception:
            logger.critical("Unable to scram authenticate with the Haystack Server.")
            raise

    def _hello_call(self) -> HelloCallResult:
        hello_headers = hello_call_headers(self.username)
        hello_result = request(self.uri + "/about", headers=hello_headers)

        return parse_hello_result(hello_result)

    def _first_call(
        self, hello_result: HelloCallResult, c1_bare: str
    ) -> FirstCallResult:
        first_headers = first_call_headers(hello_result, c1_bare)
        first_result = request(
            self.uri + "/about",
            headers=first_headers,
        )

        return parse_first_result(first_result)

    def _last_call(
        self,
        hello_result: HelloCallResult,
        c1_bare: str,
        first_result: FirstCallResult,
    ) -> str:
        last_headers = last_call_headers(
            self._password,
            hello_result,
            c1_bare,
            first_result,
        )
        last_result = request(self.uri + "/about", headers=last_headers)
        return parse_last_result(last_result)

    # ----------------------------------------------------------------------------------
    # define an optional context manager
    # ----------------------------------------------------------------------------------

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # ----------------------------------------------------------------------------------
    # standard Haystack ops
    # ----------------------------------------------------------------------------------

    def about(self) -> Grid:
        """
        Executes the Haystack about op, which queries basic information about
        the server.
        """
        grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[])
        return self.call("about", grid)

    def close(self) -> Grid:
        """
        Executes the Haystack close op, which closes the active
        authentication session.

        Note:  The close op may have side effects so we need to use HTTP POST.
        """
        grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[])
        call_result = self.call("close", grid)

        if call_result.cols[0]["name"] != "empty":
            raise InvalidCloseError(
                f"Expected an empty grid response and instead received:\n{call_result}"
            )

        return call_result

    def read(self, filter: str, limit: Optional[int] = None) -> Grid:
        if limit is None:
            grid = Grid.to_grid({"filter": filter})
        else:
            grid = Grid.to_grid({"filter": filter, "limit": limit})
        return self.call("read", grid)

    def his_read(self, ref: Ref, range: str) -> Grid:
        grid = Grid.to_grid({"id": {"_kind": "ref", "val": ref.val}, "range": range})
        return self.call("hisRead", grid)

    def his_write(self, his_grid: Grid) -> Grid:
        return self.call("hisWrite", his_grid)

    # ----------------------------------------------------------------------------------
    # other ops
    # ----------------------------------------------------------------------------------

    def eval(self, grid: Grid) -> Grid:
        return self.call("eval", grid)

    # ----------------------------------------------------------------------------------
    # base to Haystack and all other ops
    # ----------------------------------------------------------------------------------

    def call(self, op: str, grid: Grid) -> Grid:
        url = f"{self.uri}/{op}"
        format = self.format

        data = grid.to_json()

        _std_headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": format,
        }

        response = request(url=url, data=data, headers=_std_headers, method="POST")

        if response.status != 200:
            raise IncorrectHttpStatus(
                f"Expected status 200 and received status {response.status}."
            )

        # convert the response to a Haystack Grid
        response = response.to_grid()

        # log errors and where there is incomplete data
        if "err" in response.meta.keys():
            error_dis = response.meta["dis"]
            logger.debug(
                f"The server returned an error grid with this message:\n{error_dis}"
            )

        if "incomplete" in response.meta.keys():
            incomplete_dis = response.meta["incomplete"]
            logger.debug(
                f"Incomplete data was returned for these reasons:\n{incomplete_dis}"
            )

        return response
