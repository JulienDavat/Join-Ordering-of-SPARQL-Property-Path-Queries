from __future__ import annotations

from functools import cached_property
from typing import List, Optional, Set

from pattern import Pattern
from triple_pattern import TriplePattern
from filter import Filter


class JoinOrder():

    def __init__(
        self, pattern: Optional[Pattern], gearing: int = 0,
        previous: Optional[JoinOrder] = None
    ) -> None:
        self._pattern = pattern
        self._gearing = gearing
        self._previous = previous
        self._children = []
        self._cardinality = 0.0
        self._epsilon = 0.0
        self._support = 0.0
        self._estimation_time = 0.0

    @property
    def pattern(self) -> Optional[Pattern]:
        return self._pattern

    @property
    def gearing(self) -> int:
        return self._gearing

    @property
    def previous(self) -> Optional[JoinOrder]:
        return self._previous

    @property
    def children(self) -> List[JoinOrder]:
        return self._children

    @property
    def cardinality(self) -> float:
        return self._cardinality

    @cardinality.setter
    def cardinality(self, value: float) -> None:
        self._cardinality = value

    @property
    def epsilon(self) -> float:
        return self._epsilon

    @epsilon.setter
    def epsilon(self, value: float) -> None:
        self._epsilon = value

    @property
    def support(self) -> float:
        return self._support

    @support.setter
    def support(self, support: float) -> None:
        self._support = support

    @property
    def estimation_time(self) -> float:
        return self._estimation_time

    @estimation_time.setter
    def estimation_time(self, time: float) -> None:
        self._estimation_time = time

    @property
    def cost(self) -> float:
        if self.previous is None:
            return self._cardinality
        return self.previous.cost + max(self.previous.cardinality, self.cardinality)

    @cached_property
    def k0(self) -> int:
        return hash(''.join(map(lambda pattern: str(pattern.id), self.get_patterns())))

    @cached_property
    def k1(self) -> int:
        if self.previous is None:
            return 0
        elif self.pattern.is_triple():
            return self.pattern.id ^ self.previous.k1
        return self.previous.k1

    @cached_property
    def k2(self) -> int:
        if self.previous is None:
            return 0
        elif self.pattern.is_triple() and self.pattern.more:
            return self.pattern.id ^ self.previous.k2
        return self.previous.k2

    @cached_property
    def size(self) -> int:
        if self.previous is None:
            return 0
        elif self.pattern.is_triple():
            return 1 + self.previous.size
        return 0 + self.previous.size

    @cached_property
    def first(self) -> Pattern:
        if self.previous.pattern is None:
            return self.pattern
        return self.previous.first

    @cached_property
    def root(self) -> JoinOrder:
        if self.previous is None:
            return self
        return self.previous.root

    @cached_property
    def variables(self) -> Set[str]:
        if self.previous is None:
            return set()
        elif self.pattern.is_triple():
            return self.previous.variables.union(self.pattern.variables)
        return self.previous.variables

    def get_patterns(self) -> List[TriplePattern]:
        if self.previous is None:
            return []
        elif self.pattern.is_triple():
            return self.previous.get_patterns() + [self.pattern]
        return self.previous.get_patterns()

    def get_filters(self) -> List[Filter]:
        if self.previous is None:
            return []
        elif self.pattern.is_filter():
            return self.previous.get_filters() + [self.pattern]
        return self.previous.get_filters()

    def compatible(self, pattern: Pattern) -> bool:
        if isinstance(pattern, TriplePattern):
            if self.size == 0:
                return True
            return len(self.variables.intersection(pattern.variables)) > 0
        return self.variables.issuperset(pattern.variables)

    def extend(
        self, pattern: Pattern, gearing: int = 0, remember: bool = True
    ) -> JoinOrder:
        join_order = JoinOrder(pattern, gearing=gearing, previous=self)
        if remember:
            self._children.append(join_order)
        return join_order

    def decompose(self) -> List[JoinOrder]:
        if self.previous is None:
            return []
        return self.previous.decompose() + [self]

    def stringify(self, target: str) -> str:
        patterns = []
        for join_order in self.decompose():
            pattern = join_order.pattern.stringify(target)
            if join_order.pattern.is_triple() and join_order.pattern.more:
                if target == 'blazegraph':
                    patterns.append(f'\t{pattern} .')
                    if join_order.gearing == 1:
                        patterns.append('\thint:Prior hint:gearing "forward" .')
                    else:
                        patterns.append('\thint:Prior hint:gearing "reverse" .')
                elif join_order.gearing == 1:
                    if join_order.pattern.object[0] != '?':
                        # if len(join_order.children) == 0:
                        #     patterns.append(f'\t{pattern[:-1]}, t_direction 3) .')
                        # else:
                        relaxed_pattern = join_order.pattern.relaxe_object()
                        pattern = relaxed_pattern.stringify(target)
                        patterns.append(f'\t{pattern[:-1]}, t_direction 1) .')
                        expr = f'IRI({relaxed_pattern.object}) = <{join_order.pattern.object}>'
                        patterns.append(f'\tFILTER ({expr}) .')
                    else:
                        patterns.append(f'\t{pattern[:-1]}, t_direction 1) .')
                else:
                    if join_order.pattern.subject[0] != '?':
                        # if len(join_order.children) == 0:
                        #     patterns.append(f'\t{pattern[:-1]}, t_direction 3) .')
                        # else:
                        relaxed_pattern = join_order.pattern.relaxe_subject()
                        pattern = relaxed_pattern.stringify(target)
                        patterns.append(f'\t{pattern[:-1]}, t_direction 2) .')
                        expr = f'IRI({relaxed_pattern.subject}) = <{join_order.pattern.subject}>'
                        patterns.append(f'\tFILTER ({expr}) .')
                    else:
                        patterns.append(f'\t{pattern[:-1]}, t_direction 2) .')
                # else:
                #     if join_order.gearing == 1:
                #         patterns.append(f'\t{pattern[:-1]}, t_direction 1) .')
                #     elif join_order.gearing == 2:
                #         patterns.append(f'\t{pattern[:-1]}, t_direction 2) .')
                #     else:
                #         patterns.append(f'\t{pattern[:-1]}, t_direction 3) .')
            else:
                patterns.append(f'\t{pattern} .')
        if target == 'blazegraph':
            patterns.insert(0, '\thint:Query hint:optimizer "None" .')
            body = '\n'.join(patterns)
            return f'SELECT DISTINCT * WHERE {{\n{body}\n}}'
        else:
            body = '\n'.join(patterns)
            return f'DEFINE sql:select-option "order" SELECT DISTINCT * WHERE {{\n{body}\n}}'

    def __lt__(self, other: JoinOrder) -> bool:
        return self.cost < other.cost

    def __eq__(self, other: JoinOrder) -> bool:
        return self.k0 == other.k0

    def __hash__(self) -> int:
        return self.k0

    def __contains__(self, item: Pattern) -> bool:
        if self.previous is None:
            return False
        elif self.pattern.id == item.id:
            return True
        return item in self.previous

    def __repr__(self) -> str:
        patterns = []
        for join_order in self.decompose():
            patterns.append(f'\t{join_order.pattern} .')
        body = '\n'.join(patterns)
        return f'SELECT DISTINCT * WHERE {{\n{body}\n}}'
