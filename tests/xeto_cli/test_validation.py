import subprocess

# import sys
from typing import Any

import pytest

from phable import Grid, Number, Ref
from phable.io.json_decoder import JsonDecoder
from phable.io.json_encoder import JsonEncoder


def get_std_out() -> Grid:
    with open("tests/xeto_cli/temp_data/stdout.json", "rb") as f:
        data = f.read()

    return JsonDecoder().decode(data)


def write_json(data: dict[str, Any]) -> str:
    encoder = JsonEncoder()
    with open("tests/xeto_cli/temp_data/temp_data.json", "w") as f:
        f.write(encoder.to_str(data))


def exec_cli_cmd(command: str) -> str:
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


def test_dict():
    # CHECK FOR PASS
    data = {
        "x": Number(2, "kW"),
        "y": Number(3, "kW"),
        "spec": Ref("phable::TestDict"),
    }

    write_json(data)

    exec_cli_cmd(
        "fan xeto fits /app/phable-test/src/temp_data/temp_data.json -outFile /app/phable-test/src/temp_data/stdout.json"
    )

    std_out = get_std_out()
    assert len(std_out.rows) == 0

    # CHECK FOR FAIL
    data = {
        "id": Ref("hi"),  # TODO: should be able to remove this id and still pass
        "x": Number(2, "kW"),
        "spec": Ref("phable::TestDict"),
    }

    write_json(data)

    exec_cli_cmd(
        "fan xeto fits /app/phable-test/src/temp_data/temp_data.json -outFile /app/phable-test/src/temp_data/stdout.json"
    )

    with pytest.raises(AssertionError):
        std_out = get_std_out()
        assert len(std_out.rows) == 0
