from abc import ABC, abstractmethod
from typing import ClassVar

from phable.kinds import PhKind


class PhCodec(ABC):
    media_type: ClassVar[str]

    @abstractmethod
    def to_str(self, data: PhKind) -> str: ...

    @abstractmethod
    def from_str(self, data: str) -> PhKind: ...

    def encode(self, data: PhKind) -> bytes:
        return self.to_str(data).encode("utf-8")

    def decode(self, data: bytes) -> PhKind:
        return self.from_str(data.decode("utf-8"))
