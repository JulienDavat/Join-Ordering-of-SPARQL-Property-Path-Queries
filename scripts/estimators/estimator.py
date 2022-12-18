from abc import ABC, abstractmethod

from join_order import JoinOrder


class CardinalityEstimator(ABC):

    @abstractmethod
    def estimate(self, join_order: JoinOrder, **kwargs) -> None:
        pass
