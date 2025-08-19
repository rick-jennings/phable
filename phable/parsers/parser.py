import json
from io import StringIO
from typing import Protocol

from phable.kinds import Grid


class HaystackParser(Protocol):
    content_type: str

    def to_grid(self, data: bytes) -> Grid: ...
    def from_grid(self, data: Grid) -> bytes: ...


class ZincParser:
    content_type = "text/zinc"

    def to_grid(self, data: bytes) -> Grid:
        from phable.parsers.zinc_reader import ZincReader

        return ZincReader(StringIO(data.decode())).read_val()

    def from_grid(self, data: Grid) -> bytes:
        from phable.parsers.zinc_writer import ZincWriter

        return ZincWriter.grid_to_str(data).encode()


class JsonParser:
    content_type = "application/json"

    def to_grid(self, data: bytes) -> Grid:
        from phable.parsers.json import json_to_grid

        return json_to_grid(json.loads(data.decode("utf-8")))

    def from_grid(self, data: Grid) -> bytes:
        from phable.parsers.json import grid_to_json

        return json.dumps(grid_to_json(data)).encode()
