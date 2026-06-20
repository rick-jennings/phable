from dataclasses import dataclass
from typing import Callable

from phable.io.ph_json import ph_from_json, ph_to_json
from phable.io.ph_zinc import ph_from_zinc, ph_to_zinc
from phable.kinds import PhKind


@dataclass(frozen=True)
class PhCodec:
    media_type: str
    encoder: Callable[[PhKind], str]
    decoder: Callable[[str], PhKind]


PH_CODECS: dict[str, PhCodec] = {
    "zinc": PhCodec("text/zinc", ph_to_zinc, ph_from_zinc),
    "json": PhCodec("application/json", ph_to_json, ph_from_json),
}
