from abc import ABC, abstractmethod
from typing import Any


class CacheBackend(ABC):
    @abstractmethod
    def load(self, key: str, expiration_minutes: int | None = None) -> Any | None:
        pass

    @abstractmethod
    def save(self, key: str, data: Any):
        pass

    @abstractmethod
    def invalidate(self, key: str):
        pass

    @abstractmethod
    def clear_all(self):
        pass
