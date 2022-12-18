from __future__ import annotations

from typing import List

from triple_pattern import TriplePattern
from filter import Filter
from multiset import Multiset


class Query():

    def __init__(
        self, name: str, patterns: List[TriplePattern], filters: List[Filter],
        multisets: List[Multiset]
    ) -> None:
        self._name = name
        self._patterns = patterns
        self._filters = filters
        self._multisets = multisets

    @property
    def name(self) -> str:
        return self._name

    @property
    def patterns(self) -> List[TriplePattern]:
        return self._patterns

    @property
    def filters(self) -> List[Filter]:
        return self._filters

    @property
    def multisets(self) -> List[Multiset]:
        return self._multisets

    @property
    def size(self) -> int:
        return len(self.patterns)

    def stringify(self, target: str) -> str:
        patterns = []
        for pattern in self.patterns:
            patterns.append('\t' + pattern.stringify(target) + ' .')
        for filter in self.filters:
            patterns.append('\t' + filter.stringify(target) + ' .')
        body = '\n'.join(patterns)
        return f'SELECT DISTINCT * WHERE {{\n{body}\n}}'

    def __repr__(self) -> str:
        patterns = []
        for pattern in self.patterns:
            patterns.append(f'\t{pattern} .')
        for filter in self.filters:
            patterns.append(f'\t{filter} .')
        body = '\n'.join(patterns)
        return f'SELECT DISTINCT * WHERE {{\n{body}\n}}'
