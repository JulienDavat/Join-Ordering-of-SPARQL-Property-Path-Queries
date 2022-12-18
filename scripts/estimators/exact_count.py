import time

from join_order import JoinOrder
from endpoint import Virtuoso
from spy import Spy
from estimators.estimator import CardinalityEstimator


class ExactCountEstimator(CardinalityEstimator):

    def __init__(self, virtuoso: Virtuoso, **kwargs) -> None:
        self._endpoint = virtuoso
        self._timeout = kwargs.get('timeout', 5000)
        self._relaxe_stars = kwargs.get('relaxe_stars', True)

    def estimate(self, join_order: JoinOrder) -> None:
        timer = time.time()
        while not join_order.pattern.is_triple():
            join_order = join_order.previous
        if join_order.gearing == 0 or join_order.size == 1:
            plan = join_order
        elif not self._relaxe_stars:
            plan = join_order
        elif join_order.gearing == 1:
            relaxed_pattern = join_order.pattern.relaxe_object()
            plan = join_order.previous.extend(
                relaxed_pattern, gearing=1, remember=False)
        else:
            relaxed_pattern = join_order.pattern.relaxe_subject()
            plan = join_order.previous.extend(
                relaxed_pattern, gearing=2, remember=False)
        query = plan.stringify('virtuoso')
        query = 'SELECT * WHERE' + query.split('WHERE')[1]
        query = query.replace(', t_direction 1', '')
        query = query.replace(', t_direction 2', '')
        try:
            spy = Spy()
            cardinality = self._endpoint.count(query, spy, timeout=self._timeout)
            if spy.get('', 'status') == 'timeout':
                cardinality = self._endpoint.count('SELECT * WHERE { ?s ?p ?o }', Spy())
                support = 0.0
            else:
                support = 1.0
        except Exception:
            cardinality = self._endpoint.count('SELECT * WHERE { ?s ?p ?o }', Spy())
            support = 0.0
        join_order.cardinality = cardinality
        join_order.support = support
        join_order.estimation_time = time.time() - timer
