import os
import subprocess
from typing import Any, Literal

from phable import Grid
from phable.io.ph_decoder import PhDecoder
from phable.io.ph_encoder import PhEncoder
from phable.io.ph_io_factory import PH_IO_FACTORY
from phable.kinds import PhKind


def fits_explain(
    recs: list[dict[str, Any]],
    *,
    io_format: Literal["json", "zinc"] = "zinc",
    graph: bool = True,
) -> Grid:
    encoder = PH_IO_FACTORY[io_format]["encoder"]
    decoder = PH_IO_FACTORY[io_format]["decoder"]

    _write_data(io_format, encoder, recs)

    cmd = f"fan xeto fits /app/phable-test/src/temp_data/temp_data.{io_format} -outFile /app/phable-test/src/temp_data/stdout.{io_format}"

    if graph:
        cmd += " -graph"

    y = _exec_cli_cmd(cmd)

    if io_format == "zinc":
        y = y[0 : y.rfind("\n")]

    x = _get_std_out_as_kind(y, decoder)

    _delete_files_in_temp_data_dir()

    return x


def _get_std_out_as_kind(data: PhKind, decoder: PhDecoder) -> Grid:
    return decoder.from_str(data)


def _write_data(data_format: str, encoder: PhEncoder, data: PhKind) -> str:
    with open(f"tests/xeto_cli/temp_data/temp_data.{data_format}", "w") as f:
        f.write(encoder.to_str(data))


def _exec_cli_cmd(command: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            "phable_haxall_cli_run",
            "/bin/bash",
            "-c",
            command,
        ],
        # stderr=sys.stdout,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _delete_files_in_temp_data_dir():
    temp_data_dir_path = "tests/xeto_cli/temp_data"

    if not os.path.isdir(temp_data_dir_path):
        raise Exception()

    for file_name in os.listdir(temp_data_dir_path):
        file_path = os.path.join(temp_data_dir_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
