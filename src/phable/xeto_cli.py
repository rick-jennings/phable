import os
import subprocess
import tempfile
from typing import Any, Literal

from phable import Grid
from phable.io.ph_codecs import PH_CODECS


class XetoCLI:
    """A client interface to Haxall's Xeto CLI tools for type checking and analysis
    of Haystack records.

    This API is experimental and subject to change in future releases.

    `XetoCLI` can be directly imported as follows:

    ```python
    from phable import XetoCLI
    ```
    """

    def __init__(
        self,
        *,
        io_format: Literal["json", "zinc"] = "zinc",
    ):
        """Initialize a `XetoCLI` instance.

        Parameters:
            io_format:
                Data serialization format for communication with Haxall. Either `json`
                or `zinc`. Defaults to `zinc`.
        """
        self._io_format = io_format
        self._encoder = PH_CODECS[io_format].encoder
        self._decoder = PH_CODECS[io_format].decoder

    def fits_explain(self, recs: list[dict[str, Any]], graph: bool = True) -> Grid:
        """Analyze records against Xeto type specifications and return detailed
        explanations.

        This method executes the Haxall `xeto fits` command to determine whether the
        provided records conform to their declared Xeto types. It returns a detailed
        explanation of type conformance, including any type mismatches or missing
        required tags.

        **Example:**

        ```python
        from phable import Marker, Number, Ref, XetoCLI

        cli = XetoCLI()
        recs = [
            {
                "dis": "Site 1",
                "site": Marker(),
                "area": Number(1_000, "square_foot"),
                "spec": Ref("ph::Site"),
            }
        ]
        result = cli.fits_explain(recs)
        ```

        Parameters:
            recs:
                List of Haystack record dictionaries to analyze. Each record should
                specify a Xeto type with a `spec` tag.
            graph:
                If `True`, includes a detailed graph output showing the type hierarchy and
                conformance details. Defaults to `True`.

        Returns:
            `Grid` explaining how each record fits (or fails to fit) its expected Xeto type specification.
        """

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=f".{self._io_format}", delete=False
        ) as temp_file:
            temp_file.write(self._encoder(recs))
            temp_file.flush()
            temp_file_path = temp_file.name

        try:
            cli_stdout = _exec_localhost_cmd(self._io_format, graph, temp_file_path)
            decoded_str = self._decoder(cli_stdout)
            assert isinstance(decoded_str, Grid)
            return decoded_str
        finally:
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass


def _exec_localhost_cmd(
    io_format: str,
    graph: bool,
    temp_file_path: str,
) -> str:
    """Execute xeto fits command on localhost."""
    cmd = [
        "xeto",
        "fits",
        temp_file_path,
        "-outFile",
        f"stdout.{io_format}",
    ]

    if graph:
        cmd.append("-graph")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    return result.stdout
