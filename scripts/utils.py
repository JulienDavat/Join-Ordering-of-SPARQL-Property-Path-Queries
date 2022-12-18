import os

from typing import List, Tuple, Union

from uuid import uuid4
from rdflib.term import Identifier, Variable, URIRef
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.paths import SequencePath, MulPath

from query import Query
from triple_pattern import TriplePattern
from filter import (
    Filter,
    Expression,
    BasicExpression,
    STRExpression,
    RelationalExpression,
    RegexExpression,
    NotExpression,
    ConditionalAndExpression,
    ConditionalOrExpression)
from multiset import Multiset


def parse_rdflib_term(term: Identifier) -> str:
    if isinstance(term, URIRef):
        return str(term)
    return term.n3()


def parse_rdflib_expression(expr: Union[Expr, Identifier]) -> Expression:
    if isinstance(expr, Identifier):
        return BasicExpression(parse_rdflib_term(expr))
    elif expr.name == 'RelationalExpression':
        left = parse_rdflib_expression(expr.expr)
        right = parse_rdflib_expression(expr.other)
        return RelationalExpression(left, expr.op, right)
    elif expr.name == 'Builtin_REGEX':
        arg = parse_rdflib_expression(expr.text)
        return RegexExpression(arg, expr.pattern)
    elif expr.name == 'Builtin_STR':
        arg = parse_rdflib_expression(expr.arg)
        return STRExpression(arg)
    elif expr.name == 'UnaryNot':
        arg = parse_rdflib_expression(expr.expr)
        return NotExpression(arg)
    elif expr.name == 'ConditionalAndExpression':
        clauses = [parse_rdflib_expression(expr.expr)]
        for expr in expr.other:
            clauses.append(parse_rdflib_expression(expr))
        return ConditionalAndExpression(clauses)
    elif expr.name == 'ConditionalOrExpression':
        clauses = [parse_rdflib_expression(expr.expr)]
        for expr in expr.other:
            clauses.append(parse_rdflib_expression(expr))
        return ConditionalOrExpression(clauses)
    raise Exception(f'Unsupported SPARQL expression: {expr.name}')


def rewrite_sequences(node: CompValue) -> CompValue:
    if node.name == 'SelectQuery':
        rewrite_sequences(node.p)
        return node
    elif node.name == 'Project':
        rewrite_sequences(node.p)
        return node
    elif node.name == 'Filter':
        rewrite_sequences(node.p)
        return node
    elif node.name == 'Join':
        rewrite_sequences(node.p1)
        rewrite_sequences(node.p2)
        return node
    elif node.name == 'BGP':
        triples = []
        for triple in node.triples:
            if isinstance(triple[1], SequencePath):
                subject = triple[0]
                predicate = triple[1].args[0]
                object = Variable(f'c{uuid4().int % 1000}')
                triples.append((subject, predicate, object))
                for i in range(1, len(triple[1].args) - 1):
                    subject = object
                    predicate = triple[1].args[i]
                    object = Variable(f'c{uuid4().int % 1000}')
                    triples.append((subject, predicate, object))
                subject = object
                predicate = triple[1].args[-1]
                object = triple[2]
                triples.append((subject, predicate, object))
            else:
                triples.append(triple)
        node.triples = triples
        return node
    elif node.name == 'ToMultiSet':
        return node
    raise Exception(f'Unsupported SPARQL operator {node.name}')


def get_patterns(node: CompValue) -> List[Tuple]:
    if node.name == 'SelectQuery':
        return get_patterns(node.p)
    elif node.name == 'Project':
        return get_patterns(node.p)
    elif node.name == 'Filter':
        return get_patterns(node.p)
    elif node.name == 'Join':
        return get_patterns(node.p1) + get_patterns(node.p2)
    elif node.name == 'BGP':
        triples = []
        for triple in node.triples:
            subject = parse_rdflib_term(triple[0])
            if isinstance(triple[1], MulPath):
                predicate = parse_rdflib_term(triple[1].path)
                zero = triple[1].zero
                more = triple[1].more
            elif isinstance(triple[1], str):
                predicate = parse_rdflib_term(triple[1])
                zero = False
                more = False
            else:
                raise Exception('Unsupported property path expression')
            object = parse_rdflib_term(triple[2])
            triples.append(TriplePattern(subject, predicate, object, zero, more))
        return triples
    elif node.name == 'ToMultiSet':
        return []
    raise Exception(f'Unsupported SPARQL operator {node.name}')


def get_filters(node: CompValue) -> List[Filter]:
    if node.name == 'SelectQuery' or node.name == 'Project':
        return get_filters(node.p)
    elif node.name == 'Filter':
        filters = get_filters(node.p)
        if node.expr.name == 'ConditionalAndExpression':
            filters.append(Filter(parse_rdflib_expression(node.expr.expr)))
            for expr in node.expr.other:
                filters.append(Filter(parse_rdflib_expression(expr)))
        else:
            filters.append(Filter(parse_rdflib_expression(node.expr)))
        return filters
    elif node.name == 'Join':
        return get_filters(node.p1) + get_filters(node.p2)
    elif node.name == 'BGP' or node.name == 'ToMultiSet':
        return []
    raise Exception(f'Unsupported SPARQL operator {node.name}')


def get_multisets(node: CompValue) -> List[Multiset]:
    if node.name == 'SelectQuery':
        return get_multisets(node.p)
    elif node.name == 'Project':
        return get_multisets(node.p)
    elif node.name == 'Filter':
        return get_multisets(node.p)
    elif node.name == 'Join':
        return get_multisets(node.p1) + get_multisets(node.p2)
    elif node.name == 'BGP':
        return []
    elif node.name == 'ToMultiSet':
        return get_multisets(node.p)
    elif node.name == 'values':
        omega = []
        for mappings in node.res:
            mu = {}
            for variable, value in mappings.items():
                mu[parse_rdflib_term(variable)] = parse_rdflib_term(value)
            omega.append(mu)
        return [Multiset(omega)]
    raise Exception(f'Unsupported SPARQL operator {node.name}')


def parse_file(file: str) -> Query:
    with open(file) as reader:
        plan = translateQuery(parseQuery(reader.read())).algebra
        plan = rewrite_sequences(plan)
        patterns = get_patterns(plan)
        filters = get_filters(plan)
        filters.extend([multiset.to_filter() for multiset in get_multisets(plan)])
        multisets = []  # get_multisets(plan)
    name = os.path.basename(file).split('.')[0]
    return Query(name, patterns, filters, multisets)
