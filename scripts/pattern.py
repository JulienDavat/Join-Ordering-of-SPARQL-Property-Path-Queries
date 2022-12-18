from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional
from uuid import uuid4


class Pattern(ABC):

    def __init__(self, id: Optional[int]) -> None:
        self._id = id if id is not None else uuid4().int

    @property
    def id(self) -> int:
        return self._id

    @abstractmethod
    def is_triple(self) -> bool:
        pass

    @abstractmethod
    def is_filter(self) -> bool:
        pass

    def __eq__(self, other: Pattern) -> bool:
        return self.id == other.id

    def __hash__(self) -> int:
        return self.id
