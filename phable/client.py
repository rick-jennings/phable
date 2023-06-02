import logging
from typing import Any, Optional

from phable.auth.scram import (
    c1_bare,
    parse_hello_call_result,
    parse_final_call_result,
    Scram,
    to_base64,
    parse_first_call_result,
)
from phable.exceptions import (
    IncorrectHttpStatus,
    InvalidCloseError,
    UnknownRecError,
    ServerSignatureNotEqualError,
    ScramAuthError,
)
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

        # attributes for scram auth
        self.handshake_token: str
        self.hash: str
        self.c1_bare: str
        self.s_nonce: str
        self.salt: str
        self.iter_count: int
        self._auth_token: str

    # ----------------------------------------------------------------------------------
    # execute the scram auth scheme to get a valid auth token from the server
    # ----------------------------------------------------------------------------------

    def open(self) -> None:
        """Initiates and executes the SCRAM authentication exchange with the server.
        Upon a successful exchange an auth token provided by the server is assigned to
        the _auth_token attribute of this class which may be used in future requests to
        the server by other class methods.
        """
        try:
            self._hello_call()
            self.c1_bare = c1_bare(self.username)
            self._first_call()
            self._final_call()
        except Exception:
            logger.critical("Unable to scram authenticate with the Haystack Server.")
            raise ScramAuthError

    def _hello_call(self) -> None:
        """Defines and sends the HELLO message to the server and processes the server's
        response according to Project Haystack's SCRAM auth instructions."""

        headers = {"Authorization": f"HELLO username={to_base64(self.username)}"}
        response = request(self.uri + "/about", headers=headers, method="GET")

        auth_header = response.headers["WWW-Authenticate"]
        self.handshake_token, self.hash = parse_hello_call_result(auth_header)

    def _first_call(self) -> None:
        """Defines and sends the "client-first-message" to the server and processes the
        server's response according to RFC5802."""

        gs2_header = "n,,"
        headers = {
            "Authorization": f"scram handshakeToken={self.handshake_token}, "
            f"hash={self.hash}, data={to_base64(gs2_header+self.c1_bare)}"
        }
        response = request(self.uri + "/about", headers=headers, method="GET")

        auth_header = response.headers["WWW-Authenticate"]
        self.s_nonce, self.salt, self.iter_count = parse_first_call_result(auth_header)

    def _final_call(self) -> None:
        """Defines and sends the "client-final-message" to the server and processes the
        server's response according to RFC5802.

        If the SCRAM authentication exchange was successful then the auth token parsed
        from the server's response is assigned to the _auth_token attribute in this
        class, which may be used in future requests to the server.

        Raises a ServerSignatureNotEqualError if the client's computed ServerSignature
        does not match the one received by the server.
        """
        sc = Scram(
            password=self._password,
            hash=self.hash,
            handshake_token=self.handshake_token,
            c1_bare=self.c1_bare,
            s_nonce=self.s_nonce,
            salt=self.salt,
            iter_count=self.iter_count,
        )
        headers = {
            "Authorization": (
                f"scram handshaketoken={self.handshake_token},"
                f"data={sc.client_final_message}"
            )
        }
        response = request(self.uri + "/about", headers=headers, method="GET")

        self._auth_token, server_signature = parse_final_call_result(response)

        if server_signature != sc.server_signature:
            raise ServerSignatureNotEqualError

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

    def his_read(self, ids: Ref | list[Ref], range: str) -> Grid:
        if isinstance(ids, Ref):
            grid = Grid.to_grid(
                {"id": {"_kind": "ref", "val": ids.val}, "range": range}
            )
        else:
            meta = {"ver": "3.0", "range": range}
            cols = [{"name": "id"}]
            rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
            grid = Grid(meta, cols, rows)

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
        response = response.grid

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
