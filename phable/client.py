import logging
from dataclasses import dataclass
from typing import Any

from phable.auth.scram import (
    Scram,
    c1_bare,
    parse_final_call_result,
    parse_first_call_result,
    parse_hello_call_result,
    to_base64,
)
from phable.http import request
from phable.kinds import DateSpan  # type: ignore
from phable.kinds import DateTimeSpan  # type: ignore
from phable.kinds import Date, DateTime, Grid, Ref
from phable.parser.json import create_his_write_grid

logger = logging.getLogger(__name__)


@dataclass
class IncorrectHttpStatus(Exception):
    help_msg: str


@dataclass
class InvalidCloseError(Exception):
    help_msg: str


@dataclass
class ScramAuthError(Exception):
    pass


@dataclass
class ServerSignatureNotEqualError(Exception):
    """Raised when the ServerSignature value sent by the server does not equal
    the ServerSignature computed by the client."""

    pass


@dataclass
class UnknownRecError(Exception):
    help_msg: str


# TODO: Consider not using Grid parameter to call method in certain cases


class Client:
    """A client interface to a Haystack Server used for authentication and
    Haystack ops.
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

        # attributes for scram auth
        self._handshake_token: str
        self._hash: str
        self._c1_bare: str
        self._s_nonce: str
        self._salt: str
        self._iter_count: int
        self._auth_token: str

    # -------------------------------------------------------------------------
    # execute the scram auth scheme to get a valid auth token from the server
    # -------------------------------------------------------------------------

    def open(self) -> None:
        """Initiates and executes the SCRAM authentication exchange with the
        server. Upon a successful exchange an auth token provided by the
        server is assigned to the _auth_token attribute of this class which
        may be used in future requests to
        the server by other class methods.
        """
        try:
            self._hello_call()
            self._c1_bare = c1_bare(self.username)
            self._first_call()
            self._final_call()
        except Exception:
            logger.critical(
                "Unable to scram authenticate with the Haystack Server."
            )
            raise ScramAuthError

    def _hello_call(self) -> None:
        """Defines and sends the HELLO message to the server and processes the
        server's response according to Project Haystack's SCRAM auth
        instructions."""

        headers = {
            "Authorization": f"HELLO username={to_base64(self.username)}"
        }
        response = request(self.uri + "/about", headers=headers, method="GET")

        self._handshake_token, self._hash = parse_hello_call_result(response)

    def _first_call(self) -> None:
        """Defines and sends the "client-first-message" to the server and
        processes the server's response according to RFC5802."""

        gs2_header = "n,,"
        headers = {
            "Authorization": f"scram handshakeToken={self._handshake_token}, "
            f"hash={self._hash}, data={to_base64(gs2_header+self._c1_bare)}"
        }
        response = request(self.uri + "/about", headers=headers, method="GET")
        self._s_nonce, self._salt, self._iter_count = parse_first_call_result(
            response
        )

    def _final_call(self) -> None:
        """Defines and sends the "client-final-message" to the server and
        processes the server's response according to RFC5802.

        If the SCRAM authentication exchange was successful then the auth
        token parsed from the server's response is assigned to the _auth_token
        attribute in this class, which may be used in future requests to the
        server.

        Raises a ServerSignatureNotEqualError if the client's computed
        ServerSignature does not match the one received by the server.
        """
        sc = Scram(
            password=self._password,
            hash=self._hash,
            handshake_token=self._handshake_token,
            c1_bare=self._c1_bare,
            s_nonce=self._s_nonce,
            salt=self._salt,
            iter_count=self._iter_count,
        )
        headers = {
            "Authorization": (
                f"scram handshaketoken={self._handshake_token},"
                f"data={sc.client_final_message}"
            )
        }
        response = request(self.uri + "/about", headers=headers, method="GET")

        self._auth_token, server_signature = parse_final_call_result(response)

        if server_signature != sc.server_signature:
            raise ServerSignatureNotEqualError

    # -------------------------------------------------------------------------
    # define an optional context manager
    # -------------------------------------------------------------------------

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        self.close()

    # -------------------------------------------------------------------------
    # standard Haystack ops
    # -------------------------------------------------------------------------

    def about(self) -> dict[str, Any]:
        """Query basic information about the server."""
        return self.call("about").rows[0]

    def close(self) -> Grid:
        """Close the connection to the Haystack server."""
        call_result = self.call("close")

        if call_result.cols[0]["name"] != "empty":
            raise InvalidCloseError(
                "Expected an empty grid response and instead received:"
                f"\n{call_result}"
            )

        return call_result

    def read(self, filter: str, limit: int | None = None) -> Grid:
        """Read a record that matches a given filter.  Apply an optional
        limit."""
        if limit is None:
            grid = Grid.to_grid({"filter": filter})
        else:
            grid = Grid.to_grid({"filter": filter, "limit": limit})
        return self.call("read", grid)

    def read_by_id(self, id: Ref) -> dict[str, Any]:
        """Read a record by its id."""
        grid = Grid.to_grid({"id": {"_kind": "ref", "val": id.val}})
        response = self.call("read", grid)

        # verify the rec was found
        if response.cols[0]["name"] == "empty":
            raise UnknownRecError(
                f"Unable to locate id {id.val} on the server."
            )

        return response.rows[0]

    def read_by_ids(self, ids: list[Ref]) -> Grid:
        """Read records by their ids."""
        parsed_ids = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
        grid = Grid.to_grid(parsed_ids)
        response = self.call("read", grid)

        # verify the recs were found
        if len(response.rows) == 0:
            raise UnknownRecError(
                "Unable to locate any of the ids on the server."
            )
        for row in response.rows:
            if len(row) == 0:
                raise UnknownRecError(
                    "Unable to locate one or more ids on the server."
                )

        return response

    def his_read(
        self,
        ids: Ref | list[Ref],
        range: Date | DateTime | DateSpan | DateTimeSpan,  # type: ignore
    ) -> Grid:
        """Read history data on selected records for the given range."""
        if isinstance(ids, Ref):
            grid = Grid.to_grid(
                {
                    "id": {"_kind": "ref", "val": ids.val},
                    "range": str(range),
                }  # type: ignore
            )
        else:
            meta = {"ver": "3.0", "range": str(range)}  # type: ignore
            cols = [{"name": "id"}]
            rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
            grid = Grid(meta, cols, rows)  # type: ignore

        return self.call("hisRead", grid)

    def his_write(
        self, ids: Ref | list[Ref], data: list[dict[str, Any]]
    ) -> Grid:
        """Write history data to records on the Haystack server.

        A Haystack Grid object defined in phable.kinds will need to be
        initialized as an arg. See reference below for more details on how to
        define the his_grid arg.
        https://project-haystack.org/doc/docHaystack/Ops#hisWrite

        Note:  Future Phable versions may apply a breaking change to this func
        to make
        it easier.
        """
        his_grid = create_his_write_grid(ids, data)
        return self.call("hisWrite", his_grid)

    # -------------------------------------------------------------------------
    # other ops
    # -------------------------------------------------------------------------

    def eval(self, grid: Grid) -> Grid:
        """Evaluates an expression."""
        return self.call("eval", grid)

    # -------------------------------------------------------------------------
    # base to Haystack and all other ops
    # -------------------------------------------------------------------------

    def call(
        self,
        op: str,
        grid: Grid = Grid(
            meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[]
        ),
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
        response = response.grid

        # log errors and where there is incomplete data
        if "err" in response.meta.keys():
            error_dis = response.meta["dis"]
            logger.debug(
                "The server returned an error grid with this message:"
                f"\n{error_dis}"
            )

        if "incomplete" in response.meta.keys():
            incomplete_dis = response.meta["incomplete"]
            logger.debug(
                "Incomplete data was returned for these reasons:"
                f"\n{incomplete_dis}"
            )

        return response
