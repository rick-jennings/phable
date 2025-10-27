from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Self,
)

from phable.auth.scram import ScramScheme
from phable.http import ph_request
from phable.io.ph_io_factory import PH_IO_FACTORY
from phable.kinds import (
    DateRange,
    DateTimeRange,
    Grid,
    Number,
    Ref,
)

if TYPE_CHECKING:
    from ssl import SSLContext


@dataclass
class CallError(Exception):
    """Error raised by `HaystackClient` when server's `Grid` response meta has an `err`
    marker tag.

    `CallError` can be directly imported as follows:

    ```python
    from phable import CallError
    ```

    Parameters:
        help_msg:
            `Grid` that has `err` marker tag in meta described
            [here](https://project-haystack.org/doc/docHaystack/HttpApi#errorGrid).
    """

    help_msg: Grid


@dataclass
class UnknownRecError(Exception):
    """Error raised by `HaystackClient` when server's `Grid` response does not include
    data for one or more recs being requested.

    `UnknownRecError` can be directly imported as follows:

    ```python
    from phable import UnknownRecError
    ```

    Parameters:
        help_msg: A display to help with troubleshooting.
    """

    help_msg: str


@contextmanager
def open_haystack_client(
    uri: str,
    username: str,
    password: str,
    *,
    content_type: str = "json",
    ssl_context: SSLContext | None = None,
) -> Generator[HaystackClient, None, None]:
    """Context manager for opening and closing a session with a Project Haystack
    defined server application. May help prevent accidentially leaving a session with
    the server open.

    `open_haystack_client` can be directly imported as follows:

    ```python
    from phable import open_haystack_client
    ```

    **Example:**

    ```python
    from phable import open_haystack_client

    uri = "http://localhost:8080/api/demo"
    with open_haystack_client(uri, "su", "password") as client:
        print(client.about())
    ```

    **Note:** This context manager uses Project Haystack's
    [close op](https://project-haystack.org/doc/docHaystack/Ops#close), which was
    later introduced. Therefore the context manager may not work with some servers.

    Parameters:
        uri: URI of endpoint such as "http://host/api/myProj/".
        username: Username for the API user.
        password: Password for the API user.
        content_type:
            Format of data exchanged via HTTP. "json" and "zinc" options are supported.
        ssl_context:
            Optional SSL context. If not provided, a SSL context with default
            settings is created and used.
    """

    client = HaystackClient.open(
        uri, username, password, content_type=content_type, ssl_context=ssl_context
    )
    yield client
    client.close()


class HaystackClient:
    """A client interface to a Project Haystack defined server application used for
    authentication and operations.

    `HaystackClient` can be directly imported as follows:

    ```python
    from phable import HaystackClient
    ```
    """

    def __init__(
        self,
        uri: str,
        auth_token: str,
        *,
        content_type: str = "json",
        ssl_context: SSLContext | None = None,
    ):
        self.uri: str = uri[0:-1] if uri[-1] == "/" else uri
        self._auth_token: str = auth_token
        self._context: SSLContext | None = ssl_context

        io_factory = PH_IO_FACTORY[content_type]
        self._ph_encoder = io_factory["encoder"]
        self._ph_decoder = io_factory["decoder"]
        self._content_type = io_factory["content_type"]

    @classmethod
    def open(
        cls,
        uri: str,
        username: str,
        password: str,
        *,
        content_type: str = "json",
        ssl_context: SSLContext | None = None,
    ) -> Self:
        """Opens a session with the server for the URI of the project.

        Raises:
            AuthError:
                Unable to authenticate with the server using the credentials provided.
            urllib.error.URLError:
                URL is not known or lacks the required http prefix.

        Parameters:
            uri: URI of endpoint such as "http://host/api/myProj/".
            username: Username for the API user.
            password: Password for the API user.
            content_type:
                Format of data exchanged via HTTP. "json" and "zinc" options are supported.
            ssl_context:
                Optional SSL context. If not provided, a SSL context with default
                settings is created and used.

        Returns:
            An instance of the class this method is used on (i.e., Client or HxClient).
        """

        scram = ScramScheme(uri, username, password, content_type, ssl_context)
        auth_token = scram.get_auth_token()

        return cls(uri, auth_token, content_type=content_type, ssl_context=ssl_context)

    def about(self) -> dict[str, Any]:
        """Query basic information about the server.

        Returns:
            A `dict` containing information about the server.
        """
        return self.call("about").rows[0]

    def close(self) -> Grid:
        """Close the connection to the server.

        **Note:** Project Haystack recently defined the Close operation. Some servers
        may not support this operation.

        Returns:
            An empty `Grid`.
        """

        return self.call("close")

    def read(self, filter: str, checked: bool = True) -> dict[Any, Any]:
        """Read from the database the first record which matches the
        [filter](https://project-haystack.org/doc/docHaystack/Filters).

        Raises:
            UnknownRecError: Server's response does not include requested rec.

        Parameters:
            filter:
                Project Haystack defined
                [filter](https://project-haystack.org/doc/docHaystack/Filters) for
                querying the server.
            checked:
                If `checked` is equal to false and the record cannot be found, an empty
                `dict` is returned. If `checked` is equal to true and the record cannot
                be found, an `UnknownRecError` is raised.

        Returns:
            An empty `dict` or a `dict` that describes the entity read.
        """
        response = self.read_all(filter, Number(1))

        if len(response.rows) == 0:
            if checked is True:
                raise UnknownRecError(
                    "Unable to locate an entity on the server that matches the filter."
                )
            else:
                response = {}
        else:
            response = response.rows[0]

        return response

    def read_all(self, filter: str, limit: int | None = None) -> Grid:
        """Read all records from the database which match the
        [filter](https://project-haystack.org/doc/docHaystack/Filters).

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

        response = self.call("read", Grid.to_grid(data_row))

        return response

    def read_by_id(self, id: Ref, checked: bool = True) -> dict[Any, Any]:
        """Read an entity record using its unique identifier.

        Raises:
            UnknownRecError: Server's response does not include requested rec.

        Parameters:
            id: Unique identifier for the record being read.
            checked:
                If `checked` is equal to false and the record cannot be found, an empty
                `dict` is returned. If `checked` is equal to true and the record cannot
                be found, an `UnknownRecError` is raised.

        Returns:
            An empty `dict` or a `dict` that describes the entity read.
        """

        data_rows = [{"id": id}]
        post_data = Grid.to_grid(data_rows)
        response = self.call("read", post_data)

        if len(response.rows) == 0:
            if checked is True:
                raise UnknownRecError("Unable to locate the id on the server.")
            else:
                response = {}
        else:
            response = response.rows[0]

        return response

    def read_by_ids(self, ids: list[Ref]) -> Grid:
        """Read a set of entity records using their unique identifiers.

        **Note:** Project Haystack recently introduced batch read support, which might
        not be supported by some servers. If your server does not support the batch
        read feature, then try using the `Client.read_by_id()` method instead.

        Raises:
            UnknownRecError: Server's response does not include requested recs.

        Parameters:
            ids: Unique identifiers for the records being read.

        Returns:
            `Grid` with a row for each entity read.
        """
        ids = ids.copy()
        data_rows = [{"id": id} for id in ids]
        post_data = Grid.to_grid(data_rows)
        response = self.call("read", post_data)

        if len(response.rows) != len(ids):
            raise UnknownRecError("Unable to locate one or more ids on the server.")
        for row in response.rows:
            if len(row) == 0:
                raise UnknownRecError("Unable to locate one or more ids on the server.")

        return response

    def his_read_by_id(
        self,
        id: Ref,
        range: date | DateRange | DateTimeRange,
    ) -> Grid:
        """Read history data associated with `id` for the given `range`.

        When there is an existing `Grid` describing point records, it is worth
        considering to use the `Client.his_read()` method to store available
        metadata within the returned `Grid`.

        **Example:**

        ```python
        from datetime import date, timedelta

        from phable import DateRange, Ref, open_haystack_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # update the id for your server
        id1 = Ref("2e749080-d2645928")

        end = date.today()
        start = end - timedelta(days=2)
        range = DateRange(start, end)

        with open_haystack_client(uri, username, password) as client:
            his_grid = client.his_read_by_id(id1, range)

        his_df_meta, his_df = his_grid.to_polars_all()
        ```

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
        response = self.call("hisRead", data)

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

        **Example:**

        ```python
        from datetime import date, timedelta

        from phable import DateRange, Ref, open_haystack_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # update the ids for your server
        id1 = Ref("2e749080-d2645928")
        id2 = Ref("2e749080-4719193b")

        end = date.today()
        start = end - timedelta(days=2)
        range = DateRange(start, end)

        with open_haystack_client(uri, username, password) as client:
            his_grid = client.his_read_by_ids([id1, id2], range)

        his_df_meta, his_df = his_grid.to_polars_all()
        ```

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
        response = self.call("hisRead", data)

        return response

    def his_write_by_id(
        self,
        id: Ref,
        his_rows: list[dict[str, Any]],
    ) -> Grid:
        """Write history data to point records on the server.

        History row key values must be valid data types defined for `Phable`.

        History row key names must be `ts` or `val`.  Values in the column named `val`
        are for the `Ref` described by the `id` parameter.

        **Additional requirements**

        1. Timestamp and value kind of `his_row` data must match the entity's (Ref)
        configured timezone and kind
        2. Numeric data must match the entity's (Ref) configured unit or status of
        being unitless

        **Recommendations for enhanced performance**

        1. Avoid posting out-of-order or duplicate data

        **Example:**

        ```python
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        from phable import Number, Ref, open_haystack_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # make sure datetime objects are timezone aware
        ts_now = datetime.now(ZoneInfo("America/New_York"))

        his_rows = [
            {
                "ts": ts_now - timedelta(seconds=30),
                "val": Number(1_000.0, "kW"),
            },
            {
                "ts": ts_now,
                "val": Number(2_000.0, "kW"),
            },
        ]

        # update the id for your server
        id1 = Ref("2e749080-3cad46c0")

        with open_haystack_client(uri, username, password) as client:
            client.his_write_by_id(id1, data_rows)
        ```

        Parameters:
            id: Unique identifier for the point record.
            his_rows: History data to be written for the `id`.

        Returns:
            An empty `Grid`.
        """
        meta = {"id": id}
        his_grid = Grid.to_grid(his_rows, meta)
        return self.call("hisWrite", his_grid)

    def his_write_by_ids(
        self,
        ids: list[Ref],
        his_rows: list[dict[str, Any]],
    ) -> Grid:
        """Write history data to point records on the server.

        History row key values must be valid data types defined for `Phable`.

        History row key names must be `ts` or `vX` where `X` is an integer equal
        to or greater than zero.  Also, `X` must not exceed the highest index of `ids`.

        The index of an id in `ids` corresponds to the column name used in `his_rows`.

        **Additional requirements**

        1. Timestamp and value kind of `his_row` data must match the entity's (Ref)
        configured timezone and kind
        2. Numeric data must match the entity's (Ref) configured unit or status of
        being unitless

        **Recommendations for enhanced performance**

        1. Avoid posting out-of-order or duplicate data

        **Batch history write support**

        Project Haystack recently defined batch history write support.  Some Project
        Haystack servers may not support writing history data to more than one point
        at a time.  For these instances it is recommended to use a `Ref` type for the
        `ids` parameter.

        **Example:**

        ```python
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        from phable import Number, Ref, open_haystack_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # make sure datetime objects are timezone aware
        ts_now = datetime.now(ZoneInfo("America/New_York"))

        his_rows = [
            {"ts": ts_now - timedelta(seconds=30), "v0": Number(1, "kW")},
            {"ts": ts_now, "v0": Number(50, "kW"), "v1": Number(20, "kW")},
        ]

        # update the ids for your server
        id1 = Ref("2e749080-3cad46c0")
        id2 = Ref("2e749080-520f621b")

        with open_haystack_client(uri, username, password) as client:
            client.his_write_by_ids([id1, id2], his_rows)
        ```

        Parameters:
            ids: Unique identifiers for the point records.
            his_rows: History data to be written for the `ids`.

        Returns:
            An empty `Grid`.
        """
        meta = {"ver": "3.0"}
        cols = [{"name": "ts"}]

        for count, id in enumerate(ids):
            cols.append({"name": f"v{count}", "meta": {"id": id}})

        his_grid = Grid(meta, cols, his_rows)

        return self.call("hisWrite", his_grid)

    def point_write(
        self,
        id: Ref,
        level: int,
        val: Number | bool | str | None = None,
        who: str | None = None,
        duration: Number | None = None,
    ) -> Grid:
        """Writes to a given level of a writable point's priority array.

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
        row = {"id": id, "level": Number(level)}

        if val is not None:
            row["val"] = val
        if who is not None:
            row["who"] = who
        if duration is not None:
            row["duration"] = duration

        return self.call("pointWrite", Grid.to_grid(row))

    def point_write_array(self, id: Ref) -> Grid:
        """Reads the current status of a writable point's priority array.

        Parameters:
            id: Unique identifier for the record.

        Returns:
            `Grid` with the server's response.
        """
        return self.call("pointWrite", Grid.to_grid({"id": id}))

    def call(
        self,
        path: str,
        data: Grid = Grid(meta={"ver": "3.0"}, cols=[{"name": "empty"}], rows=[]),
    ) -> Grid:
        """Sends a POST request to `{uri}/{path}` using provided `data`.

        This operation is not defined by Project Haystack. However, other `Client`
        methods use this method internally.

        Parameters:
            path:
                Location on endpoint such that the complete path of the request is
                `{uri}/{path}`

                **Note:** The `uri` stored in the `Client` instance and the value
                provided as the `path` parameter of this method are used.
            data:
                Data passed in the POST request.

        Raises:
            CallError:
                Error raised by `Client` when server's `Grid` response meta has an
                `err` marker tag described
                [here](https://project-haystack.org/doc/docHaystack/HttpApi#errorGrid).

        Returns:
            HTTP response.
        """
        headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": self._content_type,
        }

        data = self._ph_encoder.encode(data)

        response = self._ph_decoder.decode(
            ph_request(
                url=f"{self.uri}/{path}",
                headers=headers,
                content_type=self._content_type,
                data=data,
                method="POST",
                context=self._context,
            ).body
        )

        _validate_response_meta(response)

        return response


def _validate_response_meta(response: Grid):
    meta = response.meta
    if "err" in meta.keys():
        raise CallError(meta)


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
