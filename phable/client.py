from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from phable.auth.scram import ScramScheme
from phable.http import post
from phable.kinds import DateRange, DateTimeRange, Grid, Ref
from phable.parsers.json import grid_to_json
from phable.parsers.pandas import to_pandas

# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------


@dataclass
class HaystackCloseOpServerResponseError(Exception):
    help_msg: str


@dataclass
class HaystackHisWriteOpServerResponseError(Exception):
    help_msg: str


@dataclass
class HaystackReadOpUnknownRecError(Exception):
    help_msg: str


@dataclass
class HaystackErrorGridRespError(Exception):
    help_msg: str


@dataclass
class HaystackIncompleteDataRespError(Exception):
    help_msg: str


# -----------------------------------------------------------------------------
# Client core interface
# -----------------------------------------------------------------------------


class Client:
    """A client interface to a Haystack Server used for authentication and
    Haystack ops.
    """

    def __init__(self, uri: str, username: str, password: str):
        self.uri: str = uri
        self.username: str = username
        self._password: str = password
        self._auth_token: str

    # -------------------------------------------------------------------------
    # open the connection with the server
    # -------------------------------------------------------------------------

    def open(self) -> None:
        """Initiates and executes the SCRAM authentication exchange with the
        server. Upon a successful exchange an auth token provided by the
        server is assigned to the _auth_token attribute of this class which
        may be used in future requests to the server by other class methods.
        """
        scram = ScramScheme(self.uri, self.username, self._password)
        self._auth_token = scram.get_auth_token()
        del scram

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

    def about(self) -> pd.Series:
        """Query basic information about the server."""
        return pd.Series(self.call("about").rows[0])

    def close(self) -> None:
        """Close the connection to the Haystack server."""
        call_result = self.call("close")

        if call_result.cols[0]["name"] != "empty":
            raise HaystackCloseOpServerResponseError(
                "Expected an empty grid response and instead received:"
                f"\n{call_result}"
            )

    def read(self, filter: str, limit: int | None = None) -> pd.DataFrame:
        """Read a record that matches a given filter.  Apply an optional
        limit.
        """
        if limit is None:
            data_row = {"filter": filter}
        else:
            data_row = {"filter": filter, "limit": limit}

        data = _rows_to_grid_json(data_row)
        return to_pandas(self.call("read", data))

    def read_by_id(self, id: Ref) -> pd.Series:
        """Read a record by its id.  Raises UnknownRecError if the rec cannot
        be found.
        """

        data_row = {"id": {"_kind": "ref", "val": id.val}}
        data = _rows_to_grid_json(data_row)
        response = self.call("read", data)

        # verify the rec was found
        if response.cols[0]["name"] == "empty":
            raise HaystackReadOpUnknownRecError(
                f"Unable to locate id {id.val} on the server."
            )

        return pd.Series(response.rows[0])

    def read_by_ids(self, ids: list[Ref]) -> pd.DataFrame:
        """Read records by their ids.  Raises UnknownRecError if any of the
        recs cannot be found.
        """
        ids = ids.copy()
        data_rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
        data = _rows_to_grid_json(data_rows)
        response = self.call("read", data)

        # verify the recs were found
        if len(response.rows) == 0:
            raise HaystackReadOpUnknownRecError(
                "Unable to locate any of the ids on the server."
            )
        for row in response.rows:
            if len(row) == 0:
                raise HaystackReadOpUnknownRecError(
                    "Unable to locate one or more ids on the server."
                )

        return to_pandas(response)

    def his_read(
        self, ids: Ref | list[Ref], range: date | DateRange | DateTimeRange
    ) -> pd.DataFrame:
        """Read history data on selected records for the given range.

        Ranges are inclusive of start timestamp and exclusive of end
        timestamp.

        If a start date is provided without a defined end, then the server
        should infer the range to be from midnight of the start date to
        midnight of the day after the start date.

        See references below for more details on range.

        References:
        1.)  https://project-haystack.org/doc/docHaystack/Ops#hisRead
        2.)  https://project-haystack.org/doc/docHaystack/Zinc
        """
        if isinstance(ids, list):
            ids = ids.copy()

        # convert range to Haystack defined string
        if isinstance(range, date):
            range = range.isoformat()
        else:
            range = str(range)

        # structure data for HTTP request
        if isinstance(ids, Ref):
            data = _to_single_his_read_json(ids, range)
        elif isinstance(ids, list):
            data = _to_batch_his_read_json(ids, range)

        return to_pandas(self.call("hisRead", data))

    def his_write(self, his_grid: Grid) -> None:
        """Write history data to records on the Haystack server.

        A Haystack Grid object defined in phable.kinds will need to be
        initialized as an arg. See reference below for more details on how to
        define the his_grid arg.
        https://project-haystack.org/doc/docHaystack/Ops#hisWrite

        Note:  Future Phable versions may apply a breaking change to this func
        to make it easier.
        """
        response_grid = self.call("hisWrite", grid_to_json(his_grid))
        if "err" in response_grid.meta.keys():
            raise HaystackHisWriteOpServerResponseError(
                "The server reported an error in response to the client's "
                "HisWrite op"
            )

    # -------------------------------------------------------------------------
    # other ops
    # -------------------------------------------------------------------------

    def eval(self, expr: str) -> pd.DataFrame:
        """Evaluates an expression."""
        data = _rows_to_grid_json({"expr": expr})

        return to_pandas(self.call("eval", data))

    # -------------------------------------------------------------------------
    # base to Haystack and all other ops
    # -------------------------------------------------------------------------

    def call(
        self,
        op: str,
        data: dict[str, Any] | None = None,
    ) -> Grid:
        if data is None:
            data = _to_empty_grid_json()
        else:
            data = data.copy()

        headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": "application/json",
        }

        response = post(url=f"{self.uri}/{op}", data=data, headers=headers)
        _validate_response_meta(response.meta)

        return response


# -----------------------------------------------------------------------------
# Misc support functions for Client()
# -----------------------------------------------------------------------------


def _validate_response_meta(meta: dict[str, str]):
    if "err" in meta.keys():
        error_dis = meta["dis"]
        raise HaystackErrorGridRespError(
            "The server returned an error grid with this message:\n"
            + error_dis
        )

    if "incomplete" in meta.keys():
        incomplete_dis = meta["incomplete"]
        raise HaystackIncompleteDataRespError(
            "Incomplete data was returned for these reasons:"
            f"\n{incomplete_dis}"
        )


def _to_single_his_read_json(id: Ref, range: str) -> dict[str, Any]:
    """Creates a Grid in the JSON format using given Ref ID and range."""
    return _rows_to_grid_json(
        {"id": {"_kind": "ref", "val": id.val}, "range": range}
    )


def _to_batch_his_read_json(ids: list[Ref], range: str) -> dict[str, Any]:
    """Returns a Grid in the JSON format using given Ref IDs and range."""

    meta = {"ver": "3.0", "range": range}
    cols = [{"name": "id"}]
    rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]

    return {
        "_kind": "grid",
        "meta": meta,
        "cols": cols,
        "rows": rows,
    }


def _to_empty_grid_json() -> dict[str, Any]:
    """Returns an empty Grid in the JSON format."""
    return {
        "_kind": "grid",
        "meta": {"ver": "3.0"},
        "cols": [{"name": "empty"}],
        "rows": [],
    }


def _rows_to_grid_json(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if isinstance(rows, dict):
        rows = [rows]

    col_names: list[str] = []
    for row in rows:
        for col_name in row.keys():
            if col_name not in col_names:
                col_names.append(col_name)

    cols = [{"name": name} for name in col_names]
    meta = {"ver": "3.0"}
    rows = rows.copy()

    return {
        "_kind": "grid",
        "meta": meta,
        "cols": cols,
        "rows": rows,
    }
