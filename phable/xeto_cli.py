import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Literal

from phable import Grid
from phable.io.ph_io_factory import PH_IO_FACTORY


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
        xeto_dir: str | Path | None = None,
        io_format: Literal["json", "zinc"] = "zinc",
    ):
        """Initialize a `XetoCLI` instance.

        Parameters:
            xeto_dir:
                Optional path to a local directory with Xeto libraries.  If provided, CLI commands will
                be executed from this directory assuming a local Haxall installation is linked.  If `None`,
                commands will execute via a Docker container called `phable_haxall_cli_run` that is assumed
                to be running. The `phable_haxall_cli_run` Docker container can be built and started
                by cloning [phable](https://github.com/rick-jennings/phable) and following the instructions
                [here](https://github.com/rick-jennings/phable/blob/main/tests/xeto_cli/README.md).
            io_format:
                Data serialization format for communication with Haxall. Either `json`
                or `zinc`. Defaults to `zinc`.
        """
        if xeto_dir is not None:
            self._xeto_dir = Path(xeto_dir)
        else:
            self._xeto_dir = None
        self._io_format = io_format
        self._encoder = PH_IO_FACTORY[io_format]["encoder"]
        self._decoder = PH_IO_FACTORY[io_format]["decoder"]

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
            temp_file.write(self._encoder.to_str(recs))
            temp_file.flush()
            temp_file_path = temp_file.name

        try:
            if self._xeto_dir is None:
                cli_stdout = _exec_docker_cmd(self._io_format, graph, temp_file_path)
            else:
                cli_stdout = _exec_localhost_cmd(
                    self._io_format, graph, temp_file_path, self._xeto_dir
                )
            return self._decoder.from_str(cli_stdout)
        finally:
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass


def _exec_docker_cmd(io_format: str, graph: bool, temp_file_path: str) -> str:
    """Execute xeto fits command in phable_haxall_cli_run Docker container."""
    container_name = "phable_haxall_cli_run"
    container_bin_path = "/app/haxall/bin"
    container_temp_path = f"/tmp/temp_data.{io_format}"
    file_copied = False

    try:
        # Copy temp file into the container
        subprocess.run(
            ["docker", "cp", temp_file_path, f"{container_name}:{container_temp_path}"],
            check=True,
            capture_output=True,
        )
        file_copied = True

        cmd = [
            "docker",
            "exec",
            "-w",
            container_bin_path,
            container_name,
            "fan",
            "xeto",
            "fits",
            container_temp_path,
            "-outFile",
            f"stdout.{io_format}",
        ]

        if graph:
            cmd.append("-graph")

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    finally:
        # Clean up temp file in container
        if file_copied:
            subprocess.run(
                ["docker", "exec", container_name, "rm", "-f", container_temp_path],
                capture_output=True,
            )


def _exec_localhost_cmd(
    io_format: str,
    graph: bool,
    temp_file_path: str,
    xeto_dir: Path,
) -> str:
    """Execute xeto fits command on localhost."""
    xeto_dir = xeto_dir.expanduser().resolve()

    if not xeto_dir.exists():
        raise FileNotFoundError(f"Xeto directory not found: {xeto_dir}")

    if not xeto_dir.is_dir():
        raise NotADirectoryError(f"Xeto path is not a directory: {xeto_dir}")

    original_dir = os.getcwd()

    try:
        os.chdir(xeto_dir)
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
            check=True,
        )

        return result.stdout
    finally:
        os.chdir(original_dir)
