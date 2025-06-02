from __future__ import annotations

import mimetypes
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

from phable.haystack_client import HaystackClient
from phable.http import ph_request, request
from phable.kinds import Grid, Uri

if TYPE_CHECKING:
    from ssl import SSLContext

from io import BufferedReader


@contextmanager
def open_haxall_client(
    uri: str,
    username: str,
    password: str,
    ssl_context: SSLContext | None = None,
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

        **Example:**

        ```python
        from phable import Marker, open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # define the rec to add
        rec = [{"dis": "TestRec", "testing": Marker(), "pytest": Marker()}]

        with open_haxall_client(uri, username, password) as client:
            # commit the rec and capture response
            rec_added_grid = client.commit_add(rec)
        ```

        Parameters:
            recs: Records to be added to the database.

        Returns:
            The full tag definitions for each of the newly added records.
        """
        meta = {"commit": "add"}
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

        **Example:**

        ```python
        from phable import Ref, open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        with open_haxall_client(uri, username, password) as client:
            # query entire rec we want to modify to get the mod tag
            rec = client.read_by_id(Ref("2e9ab42e-c9822ff9"))

            # define new tag to add to rec
            rec["foo"] = "new tag"

            # commit update to rec and capture response
            rec_modified_grid = client.commit_update(rec)
        ```

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

        **Example:**

        ```python
        from phable import Ref, open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        with open_haxall_client(uri, username, password) as client:
            # query entire rec you want to delete to get the mod tag
            rec = client.read_by_id(Ref("2e9ab42e-c9822ff9"))

            # delete the rec
            client.commit_remove(rec)
        ```

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

        **Example:**

        ```python
        from phable import open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        # define an axon expression to evaluate on the server
        axon_expr = "read(power and point and equipRef->siteMeter).hisRead(lastMonth)"

        with open_haxall_client(uri, username, password) as client:
            his_grid = client.eval(axon_expr)

        his_df_meta, his_df = his_grid.to_polars_all()
        ```

        Parameters:
            expr: Axon string expression.

        Returns:
            `Grid` with the server's response.
        """
        return self.call("eval", Grid.to_grid({"expr": expr}))

    def file_get(self, remote_file_uri: str) -> BufferedReader:
        """Fetches content from a file on the server and returns a buffered binary stream.

        The data in the HTTP response is not logged since a buffered reader can only be read once.

        Phable users should manually close the returned stream as shown in the example below.

        **Note:**  This method is experimental and subject to change.

        **Example:**

        ```python
        from phable import open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        with open_haxall_client(uri, username, password) as client:
            stream = client.file_get("/proj/demo/io/data.txt")

            # write data from the stream to a local file called data.txt
            with open("data.txt", "wb") as file:
                file.write(stream.read())

            # don't forget to close the stream when finished!
            stream.close()
        ```

        Parameters:
            remote_file_uri:
                URI of the remote file that has content being fetched.

        Returns:
            A buffered binary stream that is readable.
        """
        remote_file_url = self.uri + "/file" + remote_file_uri

        mimetype = mimetypes.guess_type(remote_file_url)[0]
        if mimetype is None:
            raise ValueError

        headers = {
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": mimetype,
        }

        res = request(
            url=remote_file_url,
            headers=headers,
            context=self._context,
        )

        return BufferedReader(res)

    def file_post(self, stream: BufferedReader, remote_file_uri: str) -> dict[str, Any]:
        """Uploads a file to a project using the HTTP POST method.

        If a file with the same name already exists on the server, then the uploaded file will be renamed.

        **Note:**  This method is experimental and subject to change.

        **Example:**

        ```python
        from phable import open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        with open_haxall_client(uri, username, password) as client:
            # use stream from local file data.txt to upload file on server
            with open("data.txt", "rb") as file:
                res_data = client.file_post(file, "/proj/demo/io/data.txt")
        ```

        Raises:
            ValueError:
                Server did not return a Grid with the URI that file content was written to.

        Parameters:
            stream:
                A buffered binary stream used for writing content to the remote file.
            remote_file_uri:
                URI that file content is intended to be written to.

        Returns:
            A dictionary of data containing the URI the file content was written to.
        """
        return self._upload_file(stream, remote_file_uri, "POST")

    def file_put(self, stream: BufferedReader, remote_file_uri: str) -> dict[str, Any]:
        """Uploads a file to a project using the HTTP PUT method.

        If a file with the same name already exists on the server, then the existing file will be overwritten with the uploaded file.

        **Note:**  This method is experimental and subject to change.

        **Example:**

        ```python
        from phable import open_haxall_client

        # define these settings specific to your use case
        uri = "http://localhost:8080/api/demo"
        username = "<username>"
        password = "<password>"

        with open_haxall_client(uri, username, password) as client:
            # use stream from local file data.txt to upload file on server
            with open("data.txt", "rb") as file:
                res_data = client.file_put(file, "/proj/demo/io/data.txt")
        ```

        Raises:
            ValueError:
                Server did not return a Grid with the URI that file content was written to.

        Parameters:
            stream:
                A buffered binary stream used for writing content to the remote file.
            remote_file_uri:
                URI of the remote file that content will be written to.

        Returns:
            A dictionary of data containing the URI the file content was written to.
        """
        return self._upload_file(stream, remote_file_uri, "PUT")

    def _upload_file(
        self, stream: BufferedReader, remote_file_uri: str, http_method: str
    ) -> dict[str, Any]:
        mimetype = mimetypes.guess_type(remote_file_uri)[0]
        if mimetype is None:
            raise ValueError

        data = stream.read()
        stream.close()

        headers = {
            "Content-Type": mimetype,
            "Authorization": f"BEARER authToken={self._auth_token}",
            "Accept": "application/json",
        }

        res = ph_request(
            self.uri + "/file" + remote_file_uri,
            headers,
            data,
            method=http_method,
            context=self._context,
        ).to_grid()

        try:
            res_data = res.rows[0]
        except IndexError:
            raise ValueError

        if isinstance(res_data.get("uri"), Uri) is False:
            raise ValueError

        return res_data
