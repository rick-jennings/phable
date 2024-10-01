from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

from phable.haystack_client import HaystackClient
from phable.kinds import Grid

if TYPE_CHECKING:
    from ssl import SSLContext


@contextmanager
def open_haxall_client(
    uri: str, username: str, password: str, ssl_context: SSLContext | None = None
) -> Generator[HaxallClient, None, None]:
    """Context manager for opening and closing a session with a
    [Haxall](https://haxall.io/) application. May help prevent accidentially leaving a
    session with the server open.

    `open_haxall_client` can be directly imported as follows:

    ```python
    from phable import open_haxall_client
    ```

    **Example:**

    ```python
    from phable import open_haxall_client

    uri = "http://localhost:8080/api/demo"
    with open_haxall_client(uri, "su", "password") as client:
        print(client.about())
    ```

    **Note:** This context manager uses Project Haystack's
    [close op](https://project-haystack.org/doc/docHaystack/Ops#close), which was
    later introduced. Therefore the context manager may not work with earlier versions
    of Haxall.

    Parameters:
        uri: URI of endpoint such as "http://host/api/myProj/".
        username: Username for the API user.
        password: Password for the API user.
        ssl_context:
            Optional SSL context. If not provided, a SSL context with default
            settings is created and used.
    """

    client = HaxallClient.open(uri, username, password, ssl_context=ssl_context)
    yield client
    client.close()


class HaxallClient(HaystackClient):
    """A superset of `HaystackClient` with support for Haxall specific operations.

    Learn more about Haxall [here](https://haxall.io/).
    """

    def commit_add(self, recs: dict[str, Any] | list[dict[str, Any]] | Grid) -> Grid:
        """Adds one or more new records to the database.

        As a general rule you should not have an `id` column in your commit grid.
        However if you wish to predefine the id of the records, you can specify an `id`
        column in the commit grid.

        Commit access requires the API user to have admin permission.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `ErrorGridError` if the operation fails
        2. `IncompleteDataError` if incomplete data is being returned

        **Additional info**

        See Haxall's Commit operation docs for more details
        [here](https://haxall.io/doc/lib-hx/op~commit).

        Parameters:
            recs: Records to be added to the database.

        Returns:
            The full tag definitions for each of the newly added records.
        """
        meta = {"commit": "add"}
        if isinstance(recs, Grid):
            recs = recs.rows
        return self.call("commit", Grid.to_grid(recs, meta))

    def commit_remove(self, recs: dict[str, Any] | list[dict[str, Any]] | Grid) -> Grid:
        """Removes one or more records from the database.

        Commit access requires the API user to have admin permission.

        **Errors**

        An `ErrorGridError` is raised if any of the recs do not exist on
        the server.

        Also, after the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `ErrorGridError` if the operation fails
        2. `IncompleteDataError` if incomplete data is being returned

        **Additional info**

        See Haxall's Commit operation docs for more details
        [here](https://haxall.io/doc/lib-hx/op~commit).

        Parameters:
            recs:
                Records to be removed from the database. Each record (or row) must at
                minimum define `id` and `mod` columns.

        Returns:
            An empty `Grid`.
        """
        meta = {"commit": "remove"}
        if isinstance(recs, Grid):
            recs = recs.rows
        return self.call("commit", Grid.to_grid(recs, meta))

    def commit_update(self, recs: dict[str, Any] | list[dict[str, Any]] | Grid) -> Grid:
        """Updates one or more existing records within the database.

        Commit access requires the API user to have admin permission.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `ErrorGridError` if the operation fails
        2. `IncompleteDataError` if incomplete data is being returned

        **Additional info**

        See Haxall's Commit operation docs for more details
        [here](https://haxall.io/doc/lib-hx/op~commit).

        Parameters:
            recs:
                Existing records within the database to be updated. Each record (or
                row) must at minimum have tags for the rec's existing `id` and `mod`
                columns (defined by the server) and the columns being updated (defined
                by the client).

        Returns:
            The latest full tag definitions for each of the updated records.
        """
        meta = {"commit": "update"}
        if isinstance(recs, Grid):
            recs = recs.rows
        return self.call("commit", Grid.to_grid(recs, meta))

    def eval(self, expr: str) -> Grid:
        """Evaluates an Axon string expression.

        **Errors**

        After the request `Grid` is successfully read by the server, the server
        may respond with a `Grid` that triggers one of the following errors to be
        raised:

        1. `ErrorGridError` if the operation fails
        2. `IncompleteDataError` if incomplete data is being returned

        **Additional info**

        See Haxall's Eval operation docs for more details
        [here](https://haxall.io/doc/lib-hx/op~eval).

        Parameters:
            expr: Axon string expression.

        Returns:
            `Grid` with the server's response.
        """

        return self.call("eval", Grid.to_grid({"expr": expr}))
