from phable.io.json_codec import JsonCodec
from phable.io.ph_codec import PhCodec
from phable.io.zinc_codec import ZincCodec

PH_CODECS: dict[str, PhCodec] = {
    "zinc": ZincCodec(),
    "json": JsonCodec(),
}
