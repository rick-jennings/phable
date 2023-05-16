import logging
from typing import Any, Optional

from phable.http import request
from phable.scram import ScramClient
from phable.kinds import Grid, Ref
from phable.json_parser import json_to_grid
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class NotFoundError(Exception):
    help_msg: str


@dataclass
class IncorrectHttpStatus(Exception):
    help_msg: str


@dataclass
class InvalidCloseError(Exception):
    help_msg: str


class Phable:
    """
    A client interface to a Haystack Server used for authentication and Haystack ops.
    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        format: Optional[str] = "application/json",
    ):
        self.uri = uri
        self.username = username
        self._password = password
        self.format = format

    def __enter__(self):
        self._auth_token = ScramClient(
            self.uri + "/about", self.username, self._password
        ).auth_token

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def about(self) -> Grid:
        """
        Executes the Haystack about op, which queries basic information about
        the server.
        """
        return json_to_grid(self.call("about", "GET"))

    def close(self) -> None:
        """
        Executes the Haystack close op, which closes the active
        authentication session.

        Note:  The close op may have side effects so we need to use HTTP POST.
        """
        grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[])
        response = json_to_grid(self.call("close", "POST", grid))

        if response.cols[0]["name"] != "empty":
            raise InvalidCloseError(
                f"Expected an empty grid response and instead received:\n{response}"
            )

    # TODO:  Add limits to read commands
    def read(self, filter: str, limit: Optional[int] = None) -> Grid:
        if limit is None:
            grid = Grid.to_grid({"filter": filter})
        else:
            grid = Grid.to_grid({"filter": filter, "limit": limit})
        return json_to_grid(self.call("read", "POST", grid))

    def read_ids(self, filter: str) -> list[Ref]:
        grid = self.read(filter)
        ids = [Ref(row["id"].val, row["id"].dis) for row in grid.rows]
        return ids

    def his_read(self, ref: Ref, range: str) -> Grid:
        grid = Grid.to_grid({"id": {"_kind": "ref", "val": ref.val}, "range": range})
        return json_to_grid(self.call("hisRead", "POST", grid))

    def his_write(self, his_grid: Grid) -> None:
        # return json_to_grid(self.call("hisWrite", "POST", his_grid))
        self.call("hisWrite", "POST", his_grid)

    def eval(self, grid: Grid) -> Grid:
        return json_to_grid(self.call("eval", "POST", grid))

    def call(self, op: str, method: str, grid: Optional[Grid] = None) -> dict[str, Any]:
        # TODO:  Change the op parameter to url if that would seem more appropriate
        """Initiates a user defined GET or POST
        https://project-haystack.org/forum/topic/930
        """

        url = f"{self.uri}/{op}"

        if grid is None:
            data = None
        else:
            data = grid.to_json()

        _std_headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": self.format,
        }

        response = request(url=url, data=data, headers=_std_headers, method=method)

        if response.status != 200:
            raise IncorrectHttpStatus(
                f"Expected status 200 and received status {response.status}."
            )

        response = response.json()

        # log errors and where there is incomplete data
        if "err" in response["meta"].keys():
            error_dis = response["meta"]["dis"]
            logger.debug(
                f"The server returned an error grid with this message:\n{error_dis}"
            )

        if "incomplete" in response["meta"].keys():
            incomplete_dis = response["meta"]["incomplete"]
            logger.debug(
                f"Incomplete data was returned for these reasons:\n{incomplete_dis}"
            )

        return response
