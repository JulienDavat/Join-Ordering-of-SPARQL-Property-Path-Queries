from typing import List, Dict

from pattern import Pattern
from filter import (
    Filter,
    BasicExpression,
    RelationalExpression,
    ConditionalOrExpression,
    ConditionalAndExpression)


class Multiset(Pattern):

    def __init__(self, omega: List[Dict[str, str]]) -> None:
        super().__init__(None)
        self._omega = omega

    def is_triple(self) -> bool:
        return False

    def is_filter(self) -> bool:
        return False

    def to_filter(self) -> Filter:
        disjunctive_clauses = []
        for mappings in self._omega:
            conjunctive_clauses = []
            for variable, value in mappings.items():
                conjunctive_clauses.append(RelationalExpression(
                    BasicExpression(variable),
                    '=',
                    BasicExpression(value)))
            if len(conjunctive_clauses) > 1:
                disjunctive_clauses.append(ConditionalAndExpression(conjunctive_clauses))
            else:
                disjunctive_clauses.append(conjunctive_clauses[0])
        if len(disjunctive_clauses) > 1:
            return Filter(ConditionalOrExpression(disjunctive_clauses))
        return Filter(disjunctive_clauses[0])

    def __repr__(self) -> str:
        variables = ' '.join(list(self._omega[0].keys()))
        values = []
        for mappings in self._omega:
            values.extend([f'({v})' for v in list(mappings.values())])
        values = ' '.join(values)
        return f'VALUES ({variables}) {{ {values} }}'
