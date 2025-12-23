from abc import ABC, abstractmethod

from phable.kinds import PhKind


class PhDecoder(ABC):
    @abstractmethod
    def decode(self, data: bytes) -> PhKind:
        pass

    @abstractmethod
    def from_str(self, data: str) -> PhKind:
        pass
