from typing import Any

from phable.io.codecs import PH_CODECS
from phable.io.json_codec import JsonCodec
from phable.io.zinc_codec import ZincCodec
from phable.kinds import PhKind

_JSON_CODEC: JsonCodec = PH_CODECS["json"]
_ZINC_CODEC: ZincCodec = PH_CODECS["zinc"]


def ph_to_json(data: PhKind) -> dict[str, Any]:
    """Encode a Project Haystack kind to a Python dict using the Haystack JSON encoding
    defined [here](https://project-haystack.org/doc/docHaystack/Json).

    **Example:**
    ```python
    from phable import Marker, ph_to_json

    data = {"equip": Marker()}
    ph_to_json(data)
    # {"equip": {"_kind": "marker"}}
    ```

    Parameters:
        data: A Project Haystack kind to encode.

    Returns:
        A Python dict representation of the Haystack JSON encoding.
    """
    return _JSON_CODEC.to_dict(data)


def ph_from_json(data: dict[str, Any]) -> PhKind:
    """Decode a Python dict using the Haystack JSON encoding defined
    [here](https://project-haystack.org/doc/docHaystack/Json) to a Project Haystack
    kind.

    **Example:**
    ```python
    from phable import ph_from_json

    data = {"equip": {"_kind": "marker"}}
    ph_from_json(data)
    # {"equip": Marker()}
    ```

    Parameters:
        data: A Python dict using the Haystack JSON encoding.

    Returns:
        A Project Haystack kind.
    """
    return _JSON_CODEC.from_dict(data)


def ph_to_zinc(data: PhKind) -> str:
    """Encode a Project Haystack kind to a Zinc string using the Haystack Zinc encoding
    defined [here](https://project-haystack.org/doc/docHaystack/Zinc).

    **Example:**
    ```python
    from phable import Marker, ph_to_zinc

    data = {"equip": Marker()}
    ph_to_zinc(data)
    # '{equip}'
    ```

    Parameters:
        data: A Project Haystack kind to encode.

    Returns:
        A Zinc string representation of the Project Haystack kind.
    """
    return _ZINC_CODEC.to_str(data)


def ph_from_zinc(data: str) -> PhKind:
    """Decode a Zinc string using the Haystack Zinc encoding defined
    [here](https://project-haystack.org/doc/docHaystack/Zinc) to a Project Haystack
    kind.

    **Example:**
    ```python
    from phable import ph_from_zinc

    ph_from_zinc('{equip}')
    # {"equip": Marker()}
    ```

    Parameters:
        data: A Zinc string to decode.

    Returns:
        A Project Haystack kind.
    """
    return _ZINC_CODEC.from_str(data)
