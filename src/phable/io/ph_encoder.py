from abc import ABC, abstractmethod

from phable.kinds import PhKind


class PhEncoder(ABC):
    @abstractmethod
    def encode(self, data: PhKind) -> bytes:
        pass
