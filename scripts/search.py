import logging

from abc import ABC, abstractmethod
from typing import List

from query import Query
from join_order import JoinOrder
from estimators.estimator import CardinalityEstimator


class SearchAlgorithm(ABC):

    def __init__(self, estimator: CardinalityEstimator, **kwargs) -> None:
        self._estimator = estimator

    def expand(self, query: Query, join_order: JoinOrder) -> List[JoinOrder]:
        candidates = []
        for pattern in query.patterns:
            if join_order.previous is None:
                if not pattern.more:
                    candidates.append(join_order.extend(pattern))
                elif len(pattern.variables) < 2:  # transitive start is not allowed
                    gearing = 1 if pattern.subject[0] != '?' else 2
                    candidate = join_order.extend(pattern, gearing=gearing)
                    candidates.append(candidate)
            if pattern not in join_order and join_order.compatible(pattern):
                if pattern.more:
                    if pattern.subject in join_order.variables:
                        candidate = join_order.extend(pattern, gearing=1)
                        candidates.append(candidate)
                    if pattern.object in join_order.variables:
                        candidate = join_order.extend(pattern, gearing=2)
                        candidates.append(candidate)
                else:
                    candidates.append(join_order.extend(pattern))
        for i in range(len(candidates)):
            for filter in query.filters:
                if filter not in candidates[i] and candidates[i].compatible(filter):
                    candidates[i] = candidates[i].extend(filter)
        return candidates

    @abstractmethod
    def run(self, query: Query) -> JoinOrder:
        pass


class DPSearch(SearchAlgorithm):

    def __init__(self, estimator: CardinalityEstimator, **kwargs) -> None:
        super().__init__(estimator, **kwargs)

    def next_round(
        self, query: Query, old_plans: List[JoinOrder]
    ) -> List[JoinOrder]:
        new_plans = {}
        for old_plan in old_plans.values():
            for new_plan in self.expand(query, old_plan):
                self._estimator.estimate(new_plan)
                if new_plan.k1 not in new_plans:
                    new_plans[new_plan.k1] = new_plan
                elif new_plan < new_plans[new_plan.k1]:
                    new_plans[new_plan.k1] = new_plan
        if logging.getLogger().getEffectiveLevel() == 10:
            logging.debug('(1) ' + '///' * 50)
            for i, new_plan in enumerate(new_plans.values()):
                if i > 0:
                    logging.debug('====' + '===' * 50)
                logging.debug(f'plan: {new_plan}')
                logging.debug(f'cost: {new_plan.cost}')
                logging.debug(f'support: {new_plan.support * 100:.2f}%')
                logging.debug(f'time: {new_plan.estimation_time:.5f}s')
            logging.debug('(1) ' + '///' * 50 + '\n')
        return new_plans

    def run(self, query: Query) -> JoinOrder:
        plans = {0: JoinOrder(None)}
        round = 0
        while round < query.size:
            plans = self.next_round(query, plans)
            round += 1
        return plans.popitem()[1]


class GreedySearch(DPSearch):

    def __init__(self, estimator: CardinalityEstimator, **kwargs) -> None:
        super().__init__(estimator, **kwargs)
        self._beam_size = kwargs.get('beam_size', 5)

    def next_round(
        self, query: Query, old_beam: List[JoinOrder]
    ) -> List[JoinOrder]:
        plans = sorted(super().next_round(query, old_beam).values())
        if logging.getLogger().getEffectiveLevel() == 10:
            logging.debug('(2) ' + '///' * 50)
            for position, plan in enumerate(plans):
                if position == self._beam_size:
                    logging.debug('xxxx' + 'xxx' * 50)
                elif position > 0:
                    logging.debug('====' + '===' * 50)
                logging.debug(f'plan n°{position + 1}: {plan}')
                logging.debug(f'cost: {plan.cost}')
                logging.debug(f'support: {plan.support * 100:.2f}%')
                logging.debug(f'time: {plan.estimation_time:.5f}s')
            logging.debug('(2) ' + '///' * 50 + '\n')
        new_beam = {}
        for plan in plans[:self._beam_size]:
            new_beam[plan.k1] = plan
        return new_beam

    def run(self, query: Query) -> JoinOrder:
        beam = {0: JoinOrder(None)}
        round = 0
        while round < query.size:
            beam = self.next_round(query, beam)
            round += 1
        return beam.popitem()[1]


class HGreedySearch(DPSearch):

    def __init__(self, estimator: CardinalityEstimator, **kwargs) -> None:
        super().__init__(estimator)
        self._beam_size = kwargs.get('beam_size', 5)
        self._beam_extra = kwargs.get('beam_extra', 1)

    def next_round(
        self, query: Query, old_beam: List[JoinOrder]
    ) -> List[JoinOrder]:
        plans = sorted(super().next_round(query, old_beam).values())
        buffer = []
        seen = set()
        beam_size = self._beam_size
        for position, plan in enumerate(plans):
            if plan.k2 != 0 and plan.k2 not in seen and len(seen) < self._beam_extra:
                buffer.append((plan, -1))
                seen.add(plan.k2)
                if position >= beam_size:
                    beam_size += 1
            else:
                buffer.append((plan, position))
        plans = sorted(buffer, key=(lambda item: item[1]))
        if logging.getLogger().getEffectiveLevel() == 10:
            logging.debug('(2) ' + '/=/' * 50)
            for position, (plan, _) in enumerate(plans):
                if position == beam_size:
                    logging.debug('xxxx' + 'xxx' * 50)
                elif position > 0:
                    logging.debug('====' + '===' * 50)
                logging.debug(f'plan n°{position + 1}: {plan}')
                logging.debug(f'cost: {plan.cost}')
                logging.debug(f'support: {plan.support * 100:.2f}%')
                logging.debug(f'time: {plan.estimation_time:.5f}s')
            logging.debug('(2) ' + '/=/' * 50 + '\n')
        new_beam = {}
        for plan, _ in plans[:beam_size]:
            new_beam[plan.k1] = plan
        return new_beam

    def run(self, query: Query) -> JoinOrder:
        beam = {0: JoinOrder(None)}
        round = 0
        while round < query.size:
            beam = self.next_round(query, beam)
            round += 1
        return beam.popitem()[1]


class DummySearch(SearchAlgorithm):

    def __init__(self, estimator: CardinalityEstimator, **kwargs) -> None:
        super().__init__(estimator, **kwargs)

    def run(self, query: Query) -> JoinOrder:
        join_order = JoinOrder(None)
        while join_order.size < query.size:
            join_order = self.expand(query, join_order)[0]
        return join_order
