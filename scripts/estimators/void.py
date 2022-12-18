import time
import numpy
import math

from join_order import JoinOrder
from hdt_connector import HDTConnector
from estimators.estimator import CardinalityEstimator


class VoidEstimator(CardinalityEstimator):

    def __init__(self, database: HDTConnector, **kwargs) -> None:
        self._database = database
        self._relaxe_stars = kwargs.get('relaxe_stars', True)

    def estimate(self, join_order: JoinOrder, **kwargs) -> None:
        timer = time.time()
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
        cardinalities = []
        values = {}
        for pattern in plan.get_patterns():
            s, p, o, hs, hp, ho = pattern.to_tuple()
            if pattern.more:
                cardinality = self._database.cardinality('', hp, '')
                if pattern.subject[0] != '?':
                    cardinality /= self._database.distinct_subjects(p)
                elif pattern.object[0] != '?':
                    cardinality /= self._database.distinct_objects(p)
            else:
                cardinality = self._database.cardinality(hs, hp, ho)
            cardinalities.append(math.log10(cardinality + 1))
            values.setdefault(s, [])
            values.setdefault(o, [])
            if hs == '' and ho == '':
                values[s].append(math.log10(self._database.distinct_subjects(p) + 1))
                values[o].append(math.log10(self._database.distinct_objects(p) + 1))
            elif hs == '':
                values[s].append(math.log10(self._database.cardinality(hs, hp, ho) + 1))
            elif ho == '':
                values[o].append(math.log10(self._database.cardinality(hs, hp, ho) + 1))
        c = numpy.prod(cardinalities)
        v = 1
        for variable in values:
            if len(values[variable]) > 1:
                v *= numpy.prod(sorted(values[variable], reverse=True)[:-1])
        join_order.cardinality = c / v
        join_order.support = 1.0
        join_order.estimation_time = time.time() - timer
