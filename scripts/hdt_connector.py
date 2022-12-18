from hdt_python import HDTDocument, LazyIDIterator
from random import randint
from typing import Dict, Tuple
from functools import lru_cache


class HDTConnector():

    def __init__(self, graph: str) -> None:
        self._spo = HDTDocument(f'data/{graph}.hdt', True, True)
        self._pso = HDTDocument(f'data/{graph}.pso.hdt', True, True)
        self._void = HDTDocument(f'data/{graph}.void.hdt', True, True)

    @lru_cache(maxsize=None)
    def get_subject_id(self, term: str) -> int:
        return self._spo.get_subject_id(term)

    def get_predicate_id(self, term: str) -> int:
        return self._spo.get_predicate_id(term)

    @lru_cache(maxsize=None)
    def get_object_id(self, term: str) -> int:
        return self._spo.get_object_id(term)

    def get_term_id(self, term: str) -> int:
        id = self.get_object_id(term)
        if id == -1:
            return self.get_subject_id(term)
        return id

    @lru_cache(maxsize=None)
    def get_subject(self, id: int) -> str:
        return self._spo.get_predicate(id)

    def get_predicate(self, id: int) -> str:
        return self._spo.get_subject(id)

    @lru_cache(maxsize=None)
    def get_object(self, id: int) -> str:
        return self._spo.get_object(id)

    def get_term(self, id: int) -> str:
        term = self.get_object(id)
        if term == '':
            return self.get_subject(id)
        return term

    @lru_cache(maxsize=None)
    def create_iterator(self, s: str, p: str, o: str) -> LazyIDIterator:
        if s == '' and o == '':
            iterator = self._pso.search_triples(p, s, o)
        else:
            iterator = self._spo.search_triples(s, p, o)
        cardinality = iterator.cardinality
        if cardinality > 0:
            iterator.next()
        return iterator, cardinality

    def cardinality(self, s: str, p: str, o: str) -> int:
        _, cardinality = self.create_iterator(s, p, o)
        return cardinality

    def sample(self, triple: Tuple) -> Tuple[Dict[str, str], int]:
        s, p, o, hs, hp, ho = triple
        iterator, cardinality = self.create_iterator(hs, hp, ho)
        if cardinality == 0:
            return {}, 0
        elif cardinality > 1:
            iterator.skip(randint(0, cardinality - 1))
            iterator.next()
        mappings = {}
        if hs == '':
            if ho == '':
                mappings[s] = iterator.predicate()
            else:
                mappings[s] = iterator.subject()
        if ho == '':
            mappings[o] = iterator.object()
        return mappings, cardinality

    @lru_cache(maxsize=None)
    def create_id_iterator(self, s: int, p: int, o: int) -> LazyIDIterator:
        if s == 0 and o == 0:
            raise Exception('PSO index not supported with IDs')
        iterator = self._spo.search_ids(s, p, o)
        cardinality = iterator.cardinality
        if cardinality > 0:
            iterator.next()
        return iterator, cardinality

    def id_sample(self, triple: Tuple) -> Tuple[Dict[str, str], int]:
        s, p, o, hs, hp, ho = triple
        iterator, cardinality = self.create_id_iterator(hs, hp, ho)
        if cardinality == 0:
            return {}, 0
        if cardinality > 1:
            iterator.skip(randint(0, cardinality - 1))
            iterator.next()
        mappings = {}
        if hs == 0:
            mappings[s] = iterator.subject_id
        if ho == 0:
            mappings[o] = iterator.object_id
        return mappings, cardinality

    def distinct_subjects(self, p: str) -> int:
        iter1 = self._void.search_triples('', 'http://rdfs.org/ns/void#property', p)
        while iter1.next():
            iter2 = self._void.search_triples(
                iter1.subject(), 'http://rdfs.org/ns/void#distinctSubjects', '')
            if iter2.cardinality > 0:
                iter2.next()
                return int(iter2.object().split('^^')[0][1:-1])
        return 0

    def distinct_objects(self, p: str) -> int:
        iter1 = self._void.search_triples('', 'http://rdfs.org/ns/void#property', p)
        while iter1.next():
            iter2 = self._void.search_triples(
                iter1.subject(), 'http://rdfs.org/ns/void#distinctObjects', '')
            if iter2.cardinality > 0:
                iter2.next()
                return int(iter2.object().split('^^')[0][1:-1])
        return 0
