import logging
from typing import Any, Optional

from phable.auth.scram import (
    FirstCallResult,
    HelloCallResult,
    first_call_headers,
    gen_nonce,
    hello_call_headers,
    last_call_headers,
    parse_first_result,
    parse_hello_result,
    parse_last_result,
)
from phable.exceptions import IncorrectHttpStatus, InvalidCloseError, UnknownRecError
from phable.http import request
from phable.kinds import Grid, Ref

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
    ):
        self.uri: str = uri
        self.username: str = username
        self._password: str = password

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

    def about(self) -> dict[str, Any]:
        """
        Executes the Haystack about op, which queries basic information about
        the server.
        """
        return self.call("about").rows[0]

    def close(self) -> Grid:
        """
        Executes the Haystack close op, which closes the active
        authentication session.

        Note:  The close op may have side effects so we need to use HTTP POST.
        """
        call_result = self.call("close")

        if call_result.cols[0]["name"] != "empty":
            raise InvalidCloseError(
                f"Expected an empty grid response and instead received:\n{call_result}"
            )

        return call_result

    def read(self, filter: str, limit: Optional[int] = None) -> Grid:
        """Read by filter

        Args:
            filter (str): _description_
            limit (Optional[int], optional): _description_. Defaults to None.

        Returns:
            Grid: _description_
        """
        if limit is None:
            grid = Grid.to_grid({"filter": filter})
        else:
            grid = Grid.to_grid({"filter": filter, "limit": limit})
        return self.call("read", grid)

    def read_by_id(self, id: Ref) -> dict[str, Any]:
        """Read by id

        Args:
            id (Ref): _description_

        Returns:
            dict[str, Any]: _description_
        """
        grid = Grid.to_grid({"id": {"_kind": "ref", "val": id.val}})
        response = self.call("read", grid)

        # verify the rec was found
        if response.cols[0]["name"] == "empty":
            raise UnknownRecError(f"Unable to locate id {id.val} on the server.")

        return response.rows[0]

    def read_by_ids(self, ids: list[Ref]) -> Grid:
        """Read by ids

        Args:
            ids (list[Ref]): _description_

        Returns:
            Grid: _description_
        """
        parsed_ids = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
        grid = Grid.to_grid(parsed_ids)
        response = self.call("read", grid)

        # verify the recs were found
        if len(response.rows) == 0:
            raise UnknownRecError("Unable to locate any of the ids on the server.")
        for row in response.rows:
            if len(row) == 0:
                raise UnknownRecError("Unable to locate one or more ids on the server.")

        return response

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

    def call(
        self,
        op: str,
        grid: Grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[]),
    ) -> Grid:
        headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": "application/json",
        }

        data = {
            "_kind": "grid",
            "meta": grid.meta,
            "cols": grid.cols,
            "rows": grid.rows,
        }

        response = request(
            url=f"{self.uri}/{op}", data=data, headers=headers, method="POST"
        )

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
