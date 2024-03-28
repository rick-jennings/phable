from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from phable.auth.scram import ScramScheme
from phable.http import post
from phable.kinds import DateRange, DateTimeRange, Grid, Marker, Number, Ref
from phable.parsers.grid import merge_pt_data_to_his_grid_cols

if TYPE_CHECKING:
    from ssl import SSLContext
    from datetime import datetime

from enum import StrEnum, auto

# -----------------------------------------------------------------------------
# Module exceptions
# -----------------------------------------------------------------------------


@dataclass
class HaystackCloseOpServerResponseError(Exception):
    help_msg: str


@dataclass
class HaystackHisWriteOpParametersError(Exception):
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
        """Close the connection to the Haystack server.

        Note: Project Haystack recently defined the close op.  Some Project Haystack
        servers may not support this operation.
        """

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

        data_row = {"filter": filter}

        if limit is not None:
            data_row["limit"] = limit

        response = self._call("read", Grid.to_grid(data_row))

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
        post_data = Grid.to_grid(data_rows)
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

        Note: Project Haystack recently defined batch history read support.  Some
        Project Haystack servers may not support reading history data for more than one
        point at a time.
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

        Note: Project Haystack recently defined batch history read support.  Some
        Project Haystack servers may not support reading history data for more than one
        point at a time.
        """

        data = _create_his_read_req_data(ids, range)
        response = self._call("hisRead", data)

        return response

    def his_write_by_ids(
        self,
        ids: Ref | list[Ref],
        his_rows: list[dict[str, datetime | bool | Number | str]],
    ) -> None:
        """Write history row data to point records on the Haystack server.

        --------------------------------------------------------------------------------
        When parameter ids is a Ref:

        History row key names must be "ts" or "val".  Values for the column named "val"
        are for the Ref described by the ids parameter.

        --------------------------------------------------------------------------------
        When parameter ids is a list of Refs:

        History row key names must be "ts" or "vX" where "X" is an integer equal
        to or greater than zero.  Also, "X" must not exceed the highest index of ids.

        The index of an id in ids corresponds to the column name used in his_rows.

        For example,

        ids = [Ref("foo0"), Ref("foo1"), Ref("foo2")]
        his_rows = [{"ts": datetime.now(), "v0": Number(1, "kW"),
                     "v1": Number(23, "kW"), "v2": Number(8, "kW")}]

        - Column named "v0" corresponds to index 0 of ids, or Ref("foo0")
        - Column named "v1" corresponds to index 1 of ids, or Ref("foo1")
        - Column named "v2" corresponds to index 2 of ids, or Ref("foo2")

        --------------------------------------------------------------------------------
        A `HaystackHisWriteOpParametersError` is raised if the below condition is not
        met:

            1. Invalid column names are used in parameter his_rows

        --------------------------------------------------------------------------------
        Additional requirements which are not validated by this method:

            1. Timestamp and value kind of his_row data must match the entity's
               (Ref) configured timezone and kind
            2. Numeric data must match the entity's (Ref) configured unit or
               status of being unitless

        Note:  We are considering to add another method `Client.his_write()` in the
        future that would validate these requirements.  It would require `pt_data`
        similar to `Client.his_read()`.

        --------------------------------------------------------------------------------
        Recommendations for enhanced performance:

            1. Avoid posting out-of-order or duplicate data

        --------------------------------------------------------------------------------
        Batch history write support:

        Project Haystack recently defined batch history write support.  Some Project
        Haystack servers may not support writing history data to more than one point
        at a time.  For these instances it is recommended to use a Ref type for the ids
        parameter.
        """

        _validate_his_write_parameters(ids, his_rows)

        if isinstance(ids, Ref):
            meta = {"id": ids}
            his_grid = Grid.to_grid(his_rows, meta)

        elif isinstance(ids, list):
            meta = {"ver": "3.0"}
            cols = [{"name": "ts"}]

            for count, id in enumerate(ids):
                cols.append({"name": f"v{count}", "meta": {"id": id}})

            his_grid = Grid(meta, cols, his_rows)

        response = self._call("hisWrite", his_grid)
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

        row = {"id": id, "level": level}

        # add optional parameters to row if applicable
        if val is not None:
            row["val"] = val
        if who is not None:
            row["who"] = who
        if dur is not None:
            row["dur"] = dur

        return self._call("pointWrite", Grid.to_grid(row))

    def point_write_array(self, id: Ref) -> Grid:
        """Uses Project Haystack's PointWrite op to read the current status of a
        writable point's priority array."""

        return self._call("pointWrite", Grid.to_grid({"id": id}))

    # -------------------------------------------------------------------------
    # SkySpark ops
    # -------------------------------------------------------------------------

    def eval(self, expr: str) -> Grid:
        """Perform a SkySpark eval op.  Evaluates an expression in SkySpark and returns
        the results."""

        return self._call("eval", Grid.to_grid({"expr": expr}))

    def commit(
        self,
        data: list[dict[str, Any]],
        flag: CommitFlag,
        read_return: bool = False,
    ) -> Grid:
        """Perform a SkySpark commit op."""

        meta = {"commit": str(flag)}

        if read_return:
            meta = meta | {"readReturn": Marker()}

        return self._call("commit", Grid.to_grid(data, meta))

    # -------------------------------------------------------------------------
    # base to Haystack and SkySpark ops
    # -------------------------------------------------------------------------

    def _call(
        self,
        op: str,
        post_data: Grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[]),
    ) -> Grid:
        """Sends a POST request based on given parameters, receives a HTTP
        response, and returns JSON data."""

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


def _validate_his_write_parameters(
    ids: list[Ref] | Ref, his_rows: list[dict[str, datetime | bool | Number | str]]
):

    if isinstance(ids, list):
        # order does not matter here
        expected_col_names = [f"v{i}" for i in range(len(ids))]
        expected_col_names.append("ts")
    elif isinstance(ids, Ref):
        expected_col_names = ["ts", "val"]

    for his_row in his_rows:
        for key in his_row.keys():
            if key not in expected_col_names:
                raise HaystackHisWriteOpParametersError(
                    f'There is an invalid column name "{key}" in one of the history '
                    "rows."
                )


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
) -> Grid:
    # convert range to Haystack defined string
    if isinstance(range, date):
        range = range.isoformat()
    else:
        range = str(range)

    # structure data for HTTP request
    if isinstance(ids, Ref):
        data = Grid.to_grid({"id": ids, "range": range})
    elif isinstance(ids, list):
        data = Grid.to_grid([{"id": id} for id in ids], {"range": range})

    return data
