from __future__ import annotations

from typing import Set, Tuple, Any, Optional
from functools import cached_property, cache
from random import randint

from hdt_connector import HDTConnector
from pattern import Pattern


class TriplePattern(Pattern):

    def __init__(
        self, subject: str, predicate: str, object: str,
        zero: bool = False, more: bool = False, id: Optional[int] = None
    ) -> None:
        super().__init__(id)
        self._terms = [subject, predicate, object]
        self._zero = zero
        self._more = more

    @property
    def subject(self) -> str:
        return self._terms[0]

    @property
    def predicate(self) -> str:
        return self._terms[1]

    @property
    def object(self) -> str:
        return self._terms[2]

    @property
    def zero(self) -> bool:
        return self._zero

    @property
    def more(self) -> bool:
        return self._more

    @cached_property
    def variables(self) -> Set[str]:
        return set([term for term in self._terms if term[0] == '?'])

    def is_triple(self) -> bool:
        return True

    def is_filter(self) -> bool:
        return False

    def to_tuple(self) -> Tuple[str, str, str, str, str, str]:
        hs = '' if self.subject[0] == '?' else self.subject
        hp = '' if self.predicate[0] == '?' else self.predicate
        ho = '' if self.object[0] == '?' else self.object
        return (self.subject, self.predicate, self.object, hs, hp, ho)

    def to_id_tuple(self, database: HDTConnector) -> Tuple[Any, int, Any, int, int, int]:
        if self.subject[0] == '?':
            s, hs = self.subject, 0
        else:
            s = hs = database.get_subject_id(self.subject)
        if self.predicate[0] == '?':
            p, hp = self.predicate, 0
        else:
            p = hp = database.get_predicate_id(self.predicate)
        if self.object[0] == '?':
            o, ho = self.object, 0
        else:
            o = ho = database.get_object_id(self.object)
        return (s, p, o, hs, hp, ho)

    @cache
    def relaxe_subject(self) -> TriplePattern:
        subject = f'?v{randint(0, 1000)}'
        return TriplePattern(subject, self.predicate, self.object, self.zero, self.more)

    @cache
    def relaxe_object(self) -> TriplePattern:
        object = f'?v{randint(0, 1000)}'
        return TriplePattern(self.subject, self.predicate, object, self.zero, self.more)

    def stringify(self, target: str) -> str:
        if target == 'virtuoso' and self.more:
            subject = self.subject
            if subject.startswith('http'):
                subject = f'<{subject}>'
            predicate = self.predicate
            if predicate.startswith('http'):
                predicate = f'<{predicate}>'
            object = self.object
            if object.startswith('http'):
                object = f'<{object}>'
            mod = 0 if self.zero else 1
            option = f'OPTION(TRANSITIVE, t_distinct, t_min({mod}))'
            return f'{subject} {predicate} {object} {option}'
        return str(self)

    def __repr__(self) -> str:
        subject = self.subject
        if subject.startswith('http'):
            subject = f'<{subject}>'
        mod = ''
        if self.more:
            mod = '*' if self.zero else '+'
        object = self.object
        if object.startswith('http'):
            object = f'<{object}>'
        return f'{subject} <{self.predicate}>{mod} {object}'
