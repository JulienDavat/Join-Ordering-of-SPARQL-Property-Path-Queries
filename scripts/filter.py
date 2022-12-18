import re

from abc import ABC, abstractmethod
from typing import Dict, Set, Union, List
from functools import cached_property

from pattern import Pattern
from hdt_connector import HDTConnector


class Expression(ABC):

    @property
    def variables(self) -> Set[str]:
        return set()

    @abstractmethod
    def eval(self, mappings: Dict[str, str]) -> Union[str, int, bool]:
        pass


class BasicExpression(Expression):

    def __init__(self, term: str) -> None:
        self._term = term

    @property
    def variables(self) -> Set[str]:
        if self._term[0] == '?':
            return set([self._term])
        return set()

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> str:
        term = mappings.get(self._term, self._term)
        if isinstance(term, int):
            term = database.get_term(term)
        if '^^' in term:
            term, type = term.split('^^')
            if type == '<http://www.w3.org/2001/XMLSchema#integer>':
                return int(term[1:-1])
        return term

    def __repr__(self) -> str:
        if self._term.startswith('http'):
            return f'<{self._term}>'
        return self._term


class STRExpression(Expression):

    def __init__(self, expr: Expression) -> None:
        self._expr = expr

    @property
    def variables(self) -> Set[str]:
        return self._expr.variables

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> str:
        return self._expr.eval(mappings, database)

    def __repr__(self) -> str:
        return f'STR({self._expr})'


class NotExpression(Expression):

    def __init__(self, expr: Expression) -> None:
        self._expr = expr

    @property
    def variables(self) -> Set[str]:
        return self._expr.variables

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        return not self._expr.eval(mappings, database)

    def __repr__(self) -> str:
        return f'!({self._expr})'


class RelationalExpression(Expression):

    def __init__(self, left: Expression, operator: str, right: Expression) -> None:
        self._left = left
        self._operator = operator
        self._right = right

    @property
    def variables(self) -> Set[str]:
        return self._left.variables.union(self._right.variables)

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        left = self._left.eval(mappings, database)
        right = self._right.eval(mappings, database)
        if self._operator == '=':
            return left == right
        elif self._operator == '<':
            return left < right
        elif self._operator == '>':
            return left > right
        elif self._operator == '<=':
            return left <= right
        return left >= right

    def __repr__(self) -> str:
        return f'{self._left} {self._operator} {self._right}'


class RegexExpression(Expression):

    def __init__(self, expr: Expression, pattern: str) -> None:
        self._expr = expr
        self._pattern = pattern

    @property
    def variables(self) -> Set[str]:
        return self._expr.variables

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        value = self._expr.eval(mappings, database)
        return len(re.findall(self._pattern, value)) > 0

    def __repr__(self) -> str:
        pattern = self._pattern
        if pattern.startswith('\\(') and pattern.endswith('\\)'):
            pattern = f'\\\\({self._pattern[2:-2]}\\\\)'
        return f'regex({self._expr}, \'{pattern}\')'


class ConditionalOrExpression(Expression):

    def __init__(self, clauses: List[Expression]) -> None:
        self._clauses = clauses

    @property
    def variables(self) -> Set[str]:
        vars = set()
        for clause in self._clauses:
            vars.update(clause.variables)
        return vars

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        for clause in self._clauses:
            if clause.eval(mappings, database):
                return True
        return False

    def __repr__(self) -> str:
        return '||'.join(map(lambda clause: f'({clause})', self._clauses))


class ConditionalAndExpression(Expression):

    def __init__(self, clauses: List[Expression]) -> None:
        self._clauses = clauses

    @property
    def variables(self) -> Set[str]:
        vars = set()
        for clause in self._clauses:
            vars.update(clause.variables)
        return vars

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        for clause in self._clauses:
            if not clause.eval(mappings, database):
                return False
        return True

    def __repr__(self) -> str:
        return '&&'.join(map(lambda clause: f'({clause})', self._clauses))


class EqExpression(Expression):

    def __init__(self, left: str, right: str) -> None:
        self._left = left
        self._right = right

    @property
    def variables(self) -> Set[str]:
        return self._left.variables.union(self._right.variables)

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        if self._left[0] != '?':
            right = mappings.get(self._right, self._right)
            if isinstance(right, int):
                left = database.get_term_id(self._left)
            else:
                left = self._left
        elif self._right[0] != '?':
            left = mappings.get(self._left, self._left)
            if isinstance(left, int):
                right = database.get_term_id(self._right)
            else:
                right = self._right
        else:
            left = mappings.get(self._left, self._left)
            right = mappings.get(self._right, self._right)
        return left == right

    def __repr__(self) -> str:
        left = f'<{self._left}>' if self._left[0] != '?' else self._left
        right = f'<{self._right}>' if self._right[0] != '?' else self._right
        return f'{left} = {right}'


class Filter(Pattern):

    def __init__(self, expression: Expression) -> None:
        super().__init__(None)
        self._expression = expression

    @cached_property
    def variables(self) -> Set[str]:
        return self._expression.variables

    def is_triple(self) -> bool:
        return False

    def is_filter(self) -> bool:
        return True

    def eval(self, mappings: Dict[str, str], database: HDTConnector) -> bool:
        return self._expression.eval(mappings, database)

    def stringify(self, target: str) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f'FILTER ({self._expression})'
