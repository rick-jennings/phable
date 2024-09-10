from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from phable.auth.scram import ScramScheme
from phable.http import post
from phable.kinds import DateRange, DateTimeRange, Grid, Number, Ref
from phable.parsers.grid import merge_pt_data_to_his_grid_cols

if TYPE_CHECKING:
    from datetime import datetime
    from ssl import SSLContext


@dataclass
class HaystackHisWriteOpParametersError(Exception):
    help_msg: str


@dataclass
class UnknownRecError(Exception):
    help_msg: str


@dataclass
class HaystackErrorGridResponseError(Exception):
    help_msg: str


@dataclass
class HaystackIncompleteDataResponseError(Exception):
    help_msg: str


class Client:
    """A client interface to a Project Haystack defined server application used for
    authentication and operations.

    The `Client` class can be directly imported as follows:

    ```python
    from phable import Client
    ```

    ## Context Manager

    An optional context manager may be used to automatically open and close the session
    with the server. This can help prevent accidentially leaving a session with the
    server open.

    **Note:** This context manager uses Project Haystack's Close operation, which was
    recently introduced. Therefore the context manager may not work with some Project
    Haystack defined servers.

    ### Example

    ```python
    from phable import Client

    # Note: Always use secure login credentials in practice!
    uri = "http://localhost:8080/api/demo"
    username = "su"
    password = "su"

    with Client(uri, username, password) as ph_client:
        print(ph_client.about())
    ```
    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        ssl_context: SSLContext | None = None,
    ):
        """
        Parameters:
            uri: URI of endpoint such as "http://host/api/myProj/".
            username: Username for the API user.
            password: Password for the API user.
            ssl_context:
                Optional SSL context. If not provided, a SSL context with default
                settings is created and used.
        """
        self.uri: str = uri
        self.username: str = username
        self._password: str = password
        self._auth_token: str
        self._context = ssl_context

    # -------------------------------------------------------------------------
    # open the connection with the server
    # -------------------------------------------------------------------------

    def open(self) -> None:
        """Initiates and executes the SCRAM authentication exchange with the server.

        Upon a successful exchange an auth token provided by the server is assigned to
        a private attribute of this class, which is used in future requests to the
        server by other class methods.

        Returns:
            `None`
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
        """Query basic information about the server.

        Returns:
            A `dict` containing information about the server.
        """
        return self._call("about").rows[0]

    def close(self) -> Grid:
        """Close the connection to the server.

        **Note:** Project Haystack recently defined the Close operation. Some servers
        may not support this operation.

        Returns:
            An empty `Grid`.
        """

        return self._call("close")

    def read(self, filter: str, checked: bool = True) -> Grid:
        """Read from the database the first record which matches the
        [filter](https://project-haystack.org/doc/docHaystack/Filters).

        **Errors**

        See `checked` parameter details.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            filter:
                Project Haystack defined
                [filter](https://project-haystack.org/doc/docHaystack/Filters) for
                querying the server.
            checked:
                If `checked` is equal to false and the record cannot be found, an empty
                `Grid` is returned. If `checked` is equal to true and the record cannot
                be found, an `UnknownRecError` is raised.

        Returns:
            An empty `Grid` or a `Grid` that has a row for the entity read.
        """

        response = self.read_all(filter, 1)

        if checked is True:
            if len(response.rows) == 0:
                raise UnknownRecError(
                    "Unable to locate an entity on the server that matches the filter."
                )

        return response

    def read_all(self, filter: str, limit: int | None = None) -> Grid:
        """Read all records from the database which match the
        [filter](https://project-haystack.org/doc/docHaystack/Filters).

        **Errors**

        After the request `Grid` is successfully read by the server, the server may
        respond with a `Grid` that triggers one of the following errors to be raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            filter:
                Project Haystack defined
                [filter](https://project-haystack.org/doc/docHaystack/Filters) for
                querying the server.
            limit: Maximum number of entities to return in response.

        Returns:
            An empty `Grid` or a `Grid` that has a row for each entity read.
        """

        data_row = {"filter": filter}

        if limit is not None:
            data_row["limit"] = limit

        response = self._call("read", Grid.to_grid(data_row))

        return response

    def read_by_id(self, id: Ref, checked: bool = True) -> Grid:
        """Read an entity record using its unique identifier.

        **Errors**

        See `checked` parameter details.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            id: Unique identifier for the record being read.
            checked:
                If `checked` is equal to false and the record cannot be found, an empty
                `Grid` is returned. If `checked` is equal to true and the record cannot
                be found, an `UnknownRecError` is raised.

        Returns:
            An empty `Grid` or a `Grid` that has a row for the entity read.
        """

        data_rows = [{"id": {"_kind": "ref", "val": id.val}}]
        post_data = Grid.to_grid(data_rows)
        response = self._call("read", post_data)

        if checked is True:
            if len(response.rows) == 0:
                raise UnknownRecError("Unable to locate the id on the server.")

        return response

    def read_by_ids(self, ids: list[Ref]) -> Grid:
        """Read a set of entity records using their unique identifiers.

        **Note:** Project Haystack recently introduced batch read support, which might
        not be supported by some servers. If your server does not support the batch
        read feature, then try using the `Client.read_by_id()` method instead.

        **Errors**

        Raises an `UnknownRecError` if any of the records cannot be found.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            ids: Unique identifiers for the records being read.

        Returns:
            `Grid` with a row for each entity read.
        """

        ids = ids.copy()
        data_rows = [{"id": {"_kind": "ref", "val": id.val}} for id in ids]
        post_data = Grid.to_grid(data_rows)
        response = self._call("read", post_data)

        if len(response.rows) == 0:
            raise UnknownRecError("Unable to locate any of the ids on the server.")
        for row in response.rows:
            if len(row) == 0:
                raise UnknownRecError("Unable to locate one or more ids on the server.")

        return response

    # TODO: raise exceptions if there are no valid pt ids on HisRead ops?
    def his_read(
        self,
        pt_data: Grid,
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Reads history data associated with `ids` within `pt_data` for the given
        `range`.

        Appends point attributes within `pt_data` as column metadata in the returned
        `Grid`.

        When there are `ids` without `pt_data`, then instead use the
        `Client.his_read_by_ids()` method.

        **Note:** Project Haystack recently defined batch history read support.  Some
        Project Haystack servers may not support reading history data for more than one
        point record at a time.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            pt_data:
                A `Grid` that contains a unique point record `id` on each row.
                Additional data for the point record is appended as column metadata in
                the returned `Grid`.
            range:
                Ranges are inclusive of start timestamp and exclusive of end timestamp.
                If a date is provided without a defined end, then the server should
                infer the range to be from midnight of the defined date to midnight of
                the day after the defined date.

        Returns:
            `Grid` with history data associated with the `ids` described in `pt_data`
            for the given `range`. The return `Grid` contains column metadata defined
            in `pt_data`.
        """

        pt_ids = [pt_row["id"] for pt_row in pt_data.rows]
        data = _create_his_read_req_data(pt_ids, range)
        response = self._call("hisRead", data)

        meta = response.meta | pt_data.meta
        cols = merge_pt_data_to_his_grid_cols(response, pt_data)
        rows = response.rows

        return Grid(meta, cols, rows)

    def his_read_by_id(
        self,
        id: Ref,
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Read history data associated with `id` for the given `range`.

        When there is an existing `Grid` describing point records, it is worth
        considering to use the `Client.his_read()` method to store available
        metadata within the returned `Grid`.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            id:
                Unique identifier for the point record associated with the requested
                history data.
            range:
                Ranges are inclusive of start timestamp and exclusive of end timestamp.
                If a date is provided without a defined end, then the server should
                infer the range to be from midnight of the defined date to midnight of
                the day after the defined date.

        Returns:
            `Grid` with history data associated with the `id` for the given `range`.
        """

        data = _create_his_read_req_data(id, range)
        response = self._call("hisRead", data)

        return response

    def his_read_by_ids(
        self,
        ids: list[Ref],
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Read history data associated with `ids` for the given `range`.

        When there is an existing `Grid` describing point records, it is worth
        considering to use the `Client.his_read()` method to store available
        metadata within the returned `Grid`.

        **Note:** Project Haystack recently defined batch history read support.  Some
        Project Haystack servers may not support reading history data for more than one
        point record at a time.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            ids:
                Unique identifiers for the point records associated with the requested
                history data.
            range:
                Ranges are inclusive of start timestamp and exclusive of end timestamp.
                If a date is provided without a defined end, then the server should
                infer the range to be from midnight of the defined date to midnight of
                the day after the defined date.

        Returns:
            `Grid` with history data associated with the `ids` for the given `range`.
        """

        data = _create_his_read_req_data(ids, range)
        response = self._call("hisRead", data)

        return response

    def his_write_by_id(
        self,
        id: Ref,
        his_rows: list[dict[str, datetime | bool | Number | str]],
    ) -> Grid:
        """Write history data to point records on the server.

        History row key names must be `ts` or `val`.  Values in the column named `val`
        are for the `Ref` described by the `id` parameter.

        **Example `his_rows`:

        ```python
        from datetime import datetime, timedelta
        from phable import Number
        from zoneinfo import ZoneInfo

        ts_now = datetime.now(ZoneInfo("America/New_York"))
        his_rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "val": Number(72.2, "kW"),
            },
            {
                "ts": ts_now,
                "val": Number(76.3, "kW"),
            },
        ]
        ```

        **Errors**

        A `HaystackHisWriteOpParametersError` is raised if invalid column names are
        used for the `his_rows` parameter.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        **Additional requirements which are not validated by this method**

        1. Timestamp and value kind of `his_row` data must match the entity's (Ref)
        configured timezone and kind
        2. Numeric data must match the entity's (Ref) configured unit or status of
        being unitless

        **Note:**  We are considering to add another method `Client.his_write()` in the
        future that would validate these requirements.  It would require `pt_data`
        similar to `Client.his_read()`.

        **Recommendations for enhanced performance**

        1. Avoid posting out-of-order or duplicate data

        Parameters:
            id: Unique identifier for the point record.
            his_rows: History data to be written for the `id`.

        Returns:
            An empty `Grid`.
        """

        _validate_his_write_parameters(id, his_rows)
        meta = {"id": id}
        his_grid = Grid.to_grid(his_rows, meta)
        return self._call("hisWrite", his_grid)

    def his_write_by_ids(
        self,
        ids: list[Ref],
        his_rows: list[dict[str, datetime | bool | Number | str]],
    ) -> Grid:
        """Write history data to point records on the server.

        History row key names must be `ts` or `vX` where `X` is an integer equal
        to or greater than zero.  Also, `X` must not exceed the highest index of `ids`.

        The index of an id in `ids` corresponds to the column name used in `his_rows`.

        **Example `his_rows`:**

        ```python
        from datetime import datetime, timedelta
        from phable import Number, Ref
        from zoneinfo import ZoneInfo

        ids = [Ref("foo0"), Ref("foo1"), Ref("foo2")]

        ts_now = datetime.now(ZoneInfo("America/New_York"))
        his_rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "v0": Number(1, "kW"),
                "v1": Number(23, "kW"),
                "v2": Number(8, "kW"),
            },
            {
                "ts": ts_now,
                "v0": Number(50, "kW"),
                "v1": Number(20, "kW"),
                "v2": Number(34, "kW"),
            }
        ]
        ```

        - Column named `v0` corresponds to index 0 of ids, or `Ref("foo0")`
        - Column named `v1` corresponds to index 1 of ids, or `Ref("foo1")`
        - Column named `v2` corresponds to index 2 of ids, or `Ref("foo2")`

        **Errors**

        A `HaystackHisWriteOpParametersError` is raised if invalid column names are
        used for the `his_rows` parameter.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        **Additional requirements which are not validated by this method**

        1. Timestamp and value kind of `his_row` data must match the entity's (Ref)
        configured timezone and kind
        2. Numeric data must match the entity's (Ref) configured unit or status of
        being unitless

        **Note:**  We are considering to add another method `Client.his_write()` in the
        future that would validate these requirements.  It would require `pt_data`
        similar to `Client.his_read()`.

        **Recommendations for enhanced performance**

        1. Avoid posting out-of-order or duplicate data

        **Batch history write support**

        Project Haystack recently defined batch history write support.  Some Project
        Haystack servers may not support writing history data to more than one point
        at a time.  For these instances it is recommended to use a `Ref` type for the
        `ids` parameter.

        Parameters:
            ids: Unique identifiers for the point records.
            his_rows: History data to be written for the `ids`.

        Returns:
            An empty `Grid`.
        """

        _validate_his_write_parameters(ids, his_rows)

        meta = {"ver": "3.0"}
        cols = [{"name": "ts"}]

        for count, id in enumerate(ids):
            cols.append({"name": f"v{count}", "meta": {"id": id}})

        his_grid = Grid(meta, cols, his_rows)

        return self._call("hisWrite", his_grid)

    def point_write(
        self,
        id: Ref,
        level: int,
        val: Number | bool | str | None = None,
        who: str | None = None,
        duration: Number | None = None,
    ) -> Grid:
        """Writes to a given level of a writable point's priority array.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            id: Unique identifier of the writable point.
            level: Integer from 1 - 17 (17 is default).
            val: Current value at level or null.
            who:
                Optional username/application name performing the write. If not
                provided, the authenticated user display name is used.
            duration: Optional number with duration unit if setting level 8.

        Returns:
            `Grid` with the server's response.
        """

        row = {"id": id, "level": level}

        if val is not None:
            row["val"] = val
        if who is not None:
            row["who"] = who
        if duration is not None:
            row["duration"] = duration

        return self._call("pointWrite", Grid.to_grid(row))

    def point_write_array(self, id: Ref) -> Grid:
        """Reads the current status of a writable point's priority array.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `HaystackErrorGridResponseError` if the operation fails
        2. `HaystackIncompleteDataResponseError` if incomplete data is being returned

        Parameters:
            id: Unique identifier for the record.

        Returns:
            `Grid` with the server's response.
        """

        return self._call("pointWrite", Grid.to_grid({"id": id}))

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
    ids: list[Ref] | Ref,
    his_rows: list[dict[str, datetime | bool | Number | str]],
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
