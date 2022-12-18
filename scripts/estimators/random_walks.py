import time
import numpy
import random

import numpy as np
import scipy.stats as st

from typing import List, Tuple, Dict
from functools import lru_cache

from query import Query
from join_order import JoinOrder
from hdt_connector import HDTConnector
from search import DPSearch, HGreedySearch
from estimators.estimator import CardinalityEstimator
from estimators.void import VoidEstimator


class RandomWalksEstimator(CardinalityEstimator):

    def __init__(self, database: HDTConnector, **kwargs) -> None:
        self._database = database
        self._num_walks = kwargs.get('num_walks', 1000)
        self._max_depth = kwargs.get('max_depth', 5)
        self._confidence = kwargs.get('confidence', 0.95)
        self._relaxe_stars = kwargs.get('relaxe_stars', True)
        self._optimize_walk_plans = kwargs.get('optimize_walk_plans', True)
        self._cache = {}

    def apply(self, triple: Tuple, mu: Dict[str, str]) -> Tuple:
        s, p, o, hs, hp, ho = triple
        return (mu.get(s, s), hp, mu.get(o, o), mu.get(s, hs), hp, mu.get(o, ho))

    def __filter_walks_with_ids__(
        self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
    ) -> List[Tuple[int, Dict[str, str], str]]:
        Y = []
        for (x_proba, x_mu, x_group) in X:
            if x_proba == 0:
                Y.append((0, x_mu, x_group))
            elif join_order.pattern.eval(x_mu, self._database):
                Y.append((x_proba, x_mu, x_group))
            else:
                Y.append((0, x_mu, x_group))
        return Y

    def __filter_walks_without_ids__(
        self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
    ) -> List[Tuple[int, Dict[str, str], str]]:
        Y = []
        for (x_proba, x_mu, x_group) in X:
            if x_proba == 0:
                Y.append((0, x_mu, x_group))
            elif join_order.pattern.eval(x_mu, self._database):
                Y.append((x_proba, x_mu, x_group))
            else:
                Y.append((0, x_mu, x_group))
        return Y

    @lru_cache(maxsize=None)
    def __compute_walks_with_ids__(
        self, join_order: JoinOrder
    ) -> List[Tuple[int, Dict[str, str], str]]:
        if join_order.previous is None:
            return [(1, {}, '') for _ in range(self._num_walks)]
        X = self.__compute_walks_with_ids__(join_order.previous)
        if join_order.pattern.is_filter():
            return self.__filter_walks_with_ids__(join_order, X)
        elif join_order.pattern.more:
            return self.__compute_closure_with_ids__(join_order, X)
        Y = []
        triple = join_order.pattern.to_id_tuple(self._database)
        for (x_proba, x_mu, x_group) in X:
            if x_proba == 0:
                Y.append((0, x_mu, x_group))
            else:
                muc, cardinality = self._database.id_sample(self.apply(triple, x_mu))
                Y.append((x_proba * cardinality, x_mu | muc, x_group))
        return Y

    @lru_cache(maxsize=None)
    def __compute_walks_without_ids__(
        self, join_order: JoinOrder
    ) -> List[Tuple[int, Dict[str, str], str]]:
        if join_order.previous is None:
            return [(1, {}, '') for _ in range(self._num_walks)]
        X = self.__compute_walks_without_ids__(join_order.previous)
        if join_order.pattern.is_filter():
            return self.__filter_walks_without_ids__(join_order, X)
        elif join_order.pattern.more:
            return self.__compute_closure_without_ids__(join_order, X)
        Y = []
        triple = join_order.pattern.to_tuple()
        for (x_proba, x_mu, x_group) in X:
            if x_proba == 0:
                Y.append((0, x_mu, x_group))
            else:
                muc, cardinality = self._database.sample(self.apply(triple, x_mu))
                Y.append((x_proba * cardinality, x_mu | muc, x_group))
        return Y

    def compute_walks(
        self, join_order: JoinOrder
    ) -> List[Tuple[int, Dict[str, str], str]]:
        _, _, _, hs, _, ho = join_order.first.to_tuple()
        if hs == '' and ho == '':
            return self.__compute_walks_without_ids__(join_order)
        return self.__compute_walks_with_ids__(join_order)

    def compute_support(
        self, walks: List[Tuple[List[int], Dict[str, str], str]]
    ) -> List[float]:
        return sum([min(1, proba) for proba, _, _ in walks]) / len(walks)

    def process_walks(
        self, walks: List[Tuple[List[int], Dict[str, str], str]]
    ) -> Tuple[List[float], List[float], List[float]]:
        groups = {}
        for (proba, mu, group) in walks:
            if group not in groups:
                groups[group] = []
            groups[group].append(proba)
        m = h = 0
        for group in groups:
            n = len(groups[group])
            if n > 1:
                matrix = np.array(groups[group])
                z = st.t.ppf((1 + self._confidence) / 2, n - 1)
                se = st.sem(matrix, axis=0, ddof=1)
                m += np.mean(matrix, axis=0)
                h += z * se
        return m, h

    def optimize_walk_plan(self, join_order: JoinOrder) -> JoinOrder:
        if join_order.k1 not in self._cache:
            query = Query('', join_order.get_patterns(), join_order.get_filters(), [])
            estimator = VoidEstimator(self._database)
            optimizer = HGreedySearch(estimator, beam_size=1, beam_extra=1)
            self._cache[join_order.k1] = optimizer.run(query)
        return self._cache[join_order.k1]

    def estimate(self, join_order: JoinOrder) -> None:
        timer = time.time()
        if join_order.size == 1 and not join_order.pattern.more:
            _, _, _, hs, hp, ho = join_order.pattern.to_tuple()
            join_order.cardinality = self._database.cardinality(hs, hp, ho)
            join_order.support = 1.0
        else:
            if join_order.gearing == 0 or join_order.size == 1:
                walk_plan = join_order
            elif not self._relaxe_stars:
                walk_plan = join_order
            elif join_order.gearing == 1:
                relaxed_pattern = join_order.pattern.relaxe_object()
                walk_plan = join_order.previous.extend(
                    relaxed_pattern, gearing=1, remember=False)
            else:
                relaxed_pattern = join_order.pattern.relaxe_subject()
                walk_plan = join_order.previous.extend(
                    relaxed_pattern, gearing=2, remember=False)
            if self._optimize_walk_plans:
                walk_plan = self.optimize_walk_plan(walk_plan)
            walks = self.compute_walks(walk_plan)
            cardinality, epsilon = self.process_walks(walks)
            join_order.cardinality = cardinality
            join_order.epsilon = epsilon
            join_order.support = self.compute_support(walks)
        join_order.estimation_time = time.time() - timer


class RandomWalksEstimator(RandomWalksEstimator):

    def __init__(self, database: HDTConnector, **kwargs) -> None:
        super().__init__(database, **kwargs)

    def __compute_closure_with_ids__(
        self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
    ) -> List[Tuple[int, Dict[str, str], str]]:
        s, p, o, hs, hp, ho = join_order.pattern.to_id_tuple(self._database)
        if join_order.gearing == 2:
            s, o = o, s
            hs, ho = ho, hs
        lowest = 0 if join_order.pattern.zero else 1
        highest = 1
        Y = []
        for x_proba, x_mu, x_group in X:
            depth = random.randint(lowest, highest)
            y_group = f'{x_group}{depth}'
            if x_proba == 0:
                Y.append((0, x_mu, y_group))
            else:
                source = x_mu[s] if hs == 0 else hs
                path = [(source, x_proba)]
                y_proba = x_proba
                max_depth = min(self._max_depth, highest, depth)
                while y_proba > 0 and len(path) <= max_depth:
                    if join_order.gearing == 1:
                        triple = (path[-1][0], p, '?node', path[-1][0], hp, 0)
                    else:
                        triple = ('?node', p, path[-1][0], 0, hp, path[-1][0])
                    muc, cardinality = self._database.id_sample(self.apply(triple, {}))
                    y_proba *= cardinality
                    if y_proba > 0:
                        if any([node == muc['?node'] for node, _ in path]):
                            y_proba = 0
                        else:
                            path.append((muc['?node'], y_proba))
                highest = max(highest, len(path))
                if depth >= len(path):
                    Y.append((0, x_mu, y_group))
                else:
                    node, y_proba = path[depth]
                    target = x_mu.get(o, 0) if ho == 0 else ho
                    if target == 0:
                        Y.append((y_proba, x_mu | {o: node}, y_group))
                    elif node == target:
                        Y.append((y_proba, x_mu, y_group))
                    else:
                        Y.append((0, x_mu, y_group))
        return Y

    def __compute_closure_without_ids__(
        self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
    ) -> List[Tuple[int, Dict[str, str], str]]:
        s, p, o, hs, hp, ho = join_order.pattern.to_tuple()
        if join_order.gearing == 2:
            s, o = o, s
            hs, ho = ho, hs
        lowest = 0 if join_order.pattern.zero else 1
        highest = 1
        Y = []
        for x_proba, x_mu, x_group in X:
            depth = random.randint(lowest, highest)
            y_group = f'{x_group}{depth}'
            if x_proba == 0:
                Y.append((0, x_mu, y_group))
            else:
                source = x_mu[s] if hs == '' else hs
                path = [(source, x_proba)]
                y_proba = x_proba
                max_depth = min(self._max_depth, highest, depth)
                while y_proba > 0 and len(path) <= max_depth:
                    if join_order.gearing == 1:
                        triple = (path[-1][0], p, '?node', path[-1][0], hp, '')
                    else:
                        triple = ('?node', p, path[-1][0], '', hp, path[-1][0])
                    muc, cardinality = self._database.sample(self.apply(triple, {}))
                    y_proba *= cardinality
                    if y_proba > 0:
                        if any([node == muc['?node'] for node, _ in path]):
                            y_proba = 0
                        else:
                            path.append((muc['?node'], y_proba))
                highest = max(highest, len(path))
                if depth >= len(path):
                    Y.append((0, x_mu, y_group))
                else:
                    node, y_proba = path[depth]
                    target = x_mu.get(o, '') if ho == '' else ho
                    if target == '':
                        Y.append((y_proba, x_mu | {o: node}, y_group))
                    elif node == target:
                        Y.append((y_proba, x_mu, y_group))
                    else:
                        Y.append((0, x_mu, y_group))
        return Y

# class RandomWalksEstimator(RandomWalksEstimator):
#
#     def __init__(self, database: HDTConnector, **kwargs) -> None:
#         super().__init__(database, **kwargs)
#
#     def __compute_closure_with_ids__(
#         self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
#     ) -> List[Tuple[int, Dict[str, str], str]]:
#         s, p, o, hs, hp, ho = join_order.pattern.to_id_tuple(self._database)
#         if join_order.gearing == 2:
#             s, o = o, s
#             hs, ho = ho, hs
#         lowest = 0 if join_order.pattern.zero else 1
#         highest = 1
#         Y = []
#         for x_proba, x_mu, x_group in X:
#             if x_proba == 0:
#                 for depth in range(lowest, highest):
#                     Y.append((0, x_mu, f'{x_group}{depth}'))
#             else:
#                 source = x_mu[s] if hs == 0 else hs
#                 target = x_mu.get(o, 0) if ho == 0 else ho
#                 path = [(source, x_proba)]
#                 y_proba = x_proba
#                 while y_proba > 0 and len(path) <= min(self._max_depth, highest):
#                     if join_order.gearing == 1:
#                         triple = (path[-1][0], p, '?node', path[-1][0], hp, 0)
#                     else:
#                         triple = ('?node', p, path[-1][0], 0, hp, path[-1][0])
#                     muc, cardinality = self._database.id_sample(self.apply(triple, {}))
#                     y_proba *= cardinality
#                     if y_proba > 0:
#                         if any([node == muc['?node'] for node, _ in path]):
#                             y_proba = 0
#                         else:
#                             path.append((muc['?node'], y_proba))
#                 highest = max(highest, len(path))
#                 for depth in range(lowest, highest):
#                     y_group = f'{x_group}{depth}'
#                     if depth >= len(path):
#                         Y.append((0, x_mu, f'{x_group}{depth}'))
#                     else:
#                         node, y_proba = path[depth]
#                         if target == 0:
#                             Y.append((y_proba, x_mu | {o: node}, y_group))
#                         elif node == target:
#                             Y.append((y_proba, x_mu, y_group))
#                         else:
#                             Y.append((0, x_mu, y_group))
#         return Y
#
#     def __compute_closure_without_ids__(
#         self, join_order: JoinOrder, X: List[Tuple[int, Dict[str, str], str]]
#     ) -> List[Tuple[int, Dict[str, str], str]]:
#         s, p, o, hs, hp, ho = join_order.pattern.to_tuple()
#         if join_order.gearing == 2:
#             s, o = o, s
#             hs, ho = ho, hs
#         lowest = 0 if join_order.pattern.zero else 1
#         highest = 1
#         Y = []
#         for x_proba, x_mu, x_group in X:
#             if x_proba == 0:
#                 for depth in range(lowest, highest):
#                     Y.append((0, x_mu, f'{x_group}{depth}'))
#             else:
#                 source = x_mu[s] if hs == '' else hs
#                 target = x_mu.get(o, '') if ho == '' else ho
#                 path = [(source, x_proba)]
#                 y_proba = x_proba
#                 while y_proba > 0 and len(path) <= min(self._max_depth, highest):
#                     if join_order.gearing == 1:
#                         triple = (path[-1][0], p, '?node', path[-1][0], hp, '')
#                     else:
#                         triple = ('?node', p, path[-1][0], '', hp, path[-1][0])
#                     muc, cardinality = self._database.sample(self.apply(triple, {}))
#                     y_proba *= cardinality
#                     if y_proba > 0:
#                         if any([node == muc['?node'] for node, _ in path]):
#                             y_proba = 0
#                         else:
#                             path.append((muc['?node'], y_proba))
#                 highest = max(highest, len(path))
#                 for depth in range(lowest, highest):
#                     y_group = f'{x_group}{depth}'
#                     if depth >= len(path):
#                         Y.append((0, x_mu, f'{x_group}{depth}'))
#                     else:
#                         node, y_proba = path[depth]
#                         if target == '':
#                             Y.append((y_proba, x_mu | {o: node}, y_group))
#                         elif node == target:
#                             Y.append((y_proba, x_mu, y_group))
#                         else:
#                             Y.append((0, x_mu, y_group))
#         return Y
#         return Y
