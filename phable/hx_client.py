from typing import Any

from phable.client import Client
from phable.kinds import Grid


class HxClient(Client):
    """A superset of `Client` with support for Haxall specific operations.

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
