from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from phable.auth.scram import ScramScheme
from phable.http import post
from phable.kinds import DateRange, DateTimeRange, Grid, Number, Ref
from phable.parsers.grid import merge_pt_data_to_his_grid_cols
from phable.parsers.json import (
    _number_to_json,
    _parse_dict_with_kinds_to_json,
    grid_to_json,
)

if TYPE_CHECKING:
    from ssl import SSLContext

from enum import StrEnum, auto

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
class HaystackErrorGridResponseError(Exception):
    help_msg: str


@dataclass
class HaystackIncompleteDataResponseError(Exception):
    help_msg: str


# -----------------------------------------------------------------------------
# Enums for string inputs
# -----------------------------------------------------------------------------


class CommitFlag(StrEnum):
    ADD = auto()
    UPDATE = auto()
    REMOVE = auto()


# -----------------------------------------------------------------------------
# Client core interface
# -----------------------------------------------------------------------------


class Client:
    """A client interface to a Haystack Server used for authentication and
    Haystack ops.

    If the optional SSL context is not provided, then a SSL context with
    default settings is created and used.
    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        ssl_context: SSLContext | None = None,
    ):
        self.uri: str = uri
        self.username: str = username
        self._password: str = password
        self._auth_token: str
        self._context = ssl_context

    # -------------------------------------------------------------------------
    # open the connection with the server
    # -------------------------------------------------------------------------

    def open(self) -> None:
        """Initiates and executes the SCRAM authentication exchange with the
        server. Upon a successful exchange an auth token provided by the
        server is assigned to the _auth_token attribute of this class which
        may be used in future requests to the server by other class methods.
        """
        scram = ScramScheme(self.uri, self.username, self._password, self._context)
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

    def about(self) -> dict[str, Any]:
        """Query basic information about the server."""
        return self._call("about").rows[0]

    def close(self) -> None:
        """Close the connection to the Haystack server."""
        call_result = self._call("close")

        if call_result.cols[0]["name"] != "empty":
            raise HaystackCloseOpServerResponseError(
                "Expected an empty grid response and instead received:"
                f"\n{call_result}"
            )

    def read(self, filter: str, limit: int | None = None) -> Grid:
        """Read a record that matches a given filter.  Apply an optional
        limit.
        """
        if limit is None:
            data_row = {"filter": filter}
        else:
            data_row = {"filter": filter, "limit": limit}

        post_data = _rows_to_grid_json(data_row)
        response = self._call("read", post_data)

        if len(response.rows) == 0:
            raise HaystackReadOpUnknownRecError(filter)

        return response

    def read_by_ids(self, ids: Ref | list[Ref]) -> Grid:
        """Read records by their ids.  Raises UnknownRecError if any of the
        recs cannot be found.
        """
        if isinstance(ids, Ref):
            ids = [ids]

        ids = ids.copy()
        data_rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
        post_data = _rows_to_grid_json(data_rows)
        response = self._call("read", post_data)

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

        return response

    # TODO: raise exceptions if there are no valid pt ids on HisRead ops?
    def his_read(
        self,
        pt_data: Grid,
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Reads history data on point IDs defined within pt_data for the given
        range.  Appends point attributes within pt_data to the column metadata
        within the returned Grid.

        Ranges are inclusive of start timestamp and exclusive of end
        timestamp. If a date is provided without a defined end, then the
        server should infer the range to be from midnight of the defined date
        to midnight of the day after the defined date.

        When there are available point IDs without pt_data, then instead use
        the Client.his_read_by_ids() method.
        """
        pt_ids = [pt_row["id"] for pt_row in pt_data.rows]
        data = _create_his_read_req_data(pt_ids, range)
        response = self._call("hisRead", data)

        meta = response.meta | pt_data.meta
        cols = merge_pt_data_to_his_grid_cols(response, pt_data)
        rows = response.rows

        return Grid(meta, cols, rows)

    def his_read_by_ids(
        self,
        ids: Ref | list[Ref],
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Read history data on defined IDs for the given range.

        Ranges are inclusive of start timestamp and exclusive of end
        timestamp. If a date is provided without a defined end, then the
        server should infer the range to be from midnight of the defined date
        to midnight of the day after the defined date.

        When there is an existing Grid describing point records, it is worth
        considering to use the Client.his_read() method to store available
        metadata within the returned Grid.
        """

        data = _create_his_read_req_data(ids, range)
        response = self._call("hisRead", data)

        return response

    def his_write(self, his_grid: Grid) -> None:
        """Write history data to records on the Haystack server.

        A Haystack Grid object defined in phable.kinds will need to be
        initialized as an arg. See reference below for more details on how to
        define the his_grid arg.
        https://project-haystack.org/doc/docHaystack/Ops#hisWrite

        Note:  Future Phable versions may apply a breaking change to this func
        to make it easier.
        """
        response = self._call("hisWrite", grid_to_json(his_grid))
        if "err" in response.meta.keys():
            raise HaystackHisWriteOpServerResponseError(
                "The server reported an error in response to the client's "
                "HisWrite op"
            )

    def point_write(
        self,
        id: Ref,
        level: int,
        val: Number | bool | str | None = None,
        who: str | None = None,
        dur: Number | None = None,
    ) -> Grid:
        """Uses Project Haystack's PointWrite op to write to a given level of a
        writable point's priority array."""

        row = {"id": {"_kind": "ref", "val": id.val}, "level": level}

        # add optional parameters to row if applicable
        if val is not None:
            if isinstance(val, Number):
                row["val"] = _number_to_json(val)
            else:
                row["val"] = val
        if who is not None:
            row["who"] = who
        if dur is not None:
            row["dur"] = _number_to_json(dur)

        data = _rows_to_grid_json([row])

        response = self._call("pointWrite", data)

        return response

    def point_write_array(self, id: Ref) -> Grid:
        """Uses Project Haystack's PointWrite op to read the current status of a
        writable point's priority array."""

        row = {"id": {"_kind": "ref", "val": id.val}}

        data = _rows_to_grid_json([row])

        response = self._call("pointWrite", data)

        return response

    # -------------------------------------------------------------------------
    # SkySpark ops
    # -------------------------------------------------------------------------

    def eval(self, expr: str) -> Grid:
        """Perform a SkySpark eval op.  Evaluates an expression in SkySpark and returns
        the results."""
        data = _rows_to_grid_json({"expr": expr})

        response = self._call("eval", data)

        return response

    def commit(
        self,
        data: list[dict[str, Any]],
        flag: CommitFlag,
        read_return: bool = False,
    ) -> Grid:
        """Perform a SkySpark commit op."""

        data = _create_commit_op_json(data, flag, read_return)

        response = self._call("commit", data)

        return response

    # -------------------------------------------------------------------------
    # base to Haystack and SkySpark ops
    # -------------------------------------------------------------------------

    def _call(
        self,
        op: str,
        post_data: dict[str, Any] | None = None,
    ) -> Grid:
        """Sends a POST request based on given parameters, receives a HTTP
        response, and returns JSON data."""

        if post_data is None:
            post_data = _to_empty_grid_json()
        else:
            post_data = post_data.copy()

        headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": "application/json",
        }

        response = post(
            url=f"{self.uri}/{op}",
            post_data=post_data,
            headers=headers,
            context=self._context,
        )
        _validate_response_meta(response.meta)

        return response


# -----------------------------------------------------------------------------
# Misc support functions for Client()
# -----------------------------------------------------------------------------


def _validate_response_meta(meta: dict[str, Any]):
    if "err" in meta.keys():
        error_dis = meta["dis"]
        raise HaystackErrorGridResponseError(
            "The server returned an error grid with this message:\n" + error_dis
        )

    if "incomplete" in meta.keys():
        incomplete_dis = meta["incomplete"]
        raise HaystackIncompleteDataResponseError(
            "Incomplete data was returned for these reasons:" f"\n{incomplete_dis}"
        )


def _create_his_read_req_data(
    ids: Ref | list[Ref], range: date | DateRange | DateTimeRange
) -> dict[str, Any]:
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

    return data


def _to_single_his_read_json(id: Ref, range: str) -> dict[str, Any]:
    """Creates a Grid in the JSON format using given Ref ID and range."""
    return _rows_to_grid_json({"id": {"_kind": "ref", "val": id.val}, "range": range})


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


def _create_commit_op_json(
    data: list[dict[str, Any]], type: CommitFlag, read_return: bool
):

    meta = {"ver": "3.0", "commit": str(type)}

    if read_return:
        meta["readReturn"] = {"_kind": "marker"}

    cols = _get_cols_from_rows(data)
    rows = [_parse_dict_with_kinds_to_json(row) for row in data]

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


def _get_cols_from_rows(rows: list[dict[str, Any]]) -> list:

    col_names: list[str] = []
    for row in rows:
        for col_name in row.keys():
            if col_name not in col_names:
                col_names.append(col_name)

    cols = [{"name": name} for name in col_names]
    return cols


def _rows_to_grid_json(rows: list[dict[str, Any]]) -> dict[str, Any]:

    if isinstance(rows, dict):
        rows = [rows]

    cols = _get_cols_from_rows(rows)
    meta = {"ver": "3.0"}
    rows = rows.copy()

    return {
        "_kind": "grid",
        "meta": meta,
        "cols": cols,
        "rows": rows,
    }
