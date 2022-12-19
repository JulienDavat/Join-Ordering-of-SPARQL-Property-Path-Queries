import logging
import time
import subprocess
import tempfile
import re

from abc import ABC, abstractmethod
from typing import Dict
from SPARQLWrapper import SPARQLWrapper, JSON

from spy import Spy


class Endpoint(ABC):

    def __init__(self, url: str, default_graph: str) -> None:
        self._url = url
        self._default_graph = default_graph

    @property
    def url(self) -> str:
        return self._url

    @property
    def default_graph(self) -> str:
        return self._default_graph

    @abstractmethod
    def execute(
        self, query: str, spy: Spy, force_order: bool = False, timeout: int = 0
    ) -> Dict:
        pass

    def count(
        self, query: str, spy: Spy, force_order: bool = False, distinct: bool = False,
        timeout: int = 0
    ) -> int:
        if distinct:
            select = 'SELECT (COUNT(DISTINCT *) AS ?count) WHERE '
        else:
            select = 'SELECT (COUNT(*) AS ?count) WHERE '
        query = select + query.split('WHERE')[1]
        solutions = self.execute(query, spy, force_order=force_order, timeout=timeout)
        num_solutions = int(solutions['results']['bindings'][0]['count']['value'])
        if spy.get('', 'status') == 'ok':
            spy.report('', 'num_solutions', num_solutions)
            return num_solutions
        return 0


class Virtuoso(Endpoint):

    class ISQLWrapper(object):

        def __init__(self, hostname: str, username: str, password: str) -> None:
            self.hostname = hostname
            self.username = username
            self.password = password

        def execute_script(self, script: str) -> str:
            cmd = ['isql', self.hostname, self.username, self.password, script]
            process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if process.stderr:
                raise Exception(process.stderr)
            return process.stdout.decode('utf-8')

        def execute_cmd(self, cmd: str) -> str:
            if not cmd.endswith(';'):
                cmd += ';'
            tf_query = tempfile.NamedTemporaryFile()
            tf_query.write(cmd.encode('utf-8'))
            tf_query.flush()
            result = self.execute_script(tf_query.name)
            tf_query.close()
            return result

        def sparql_query(self, query: str) -> str:
            if not query.endswith(';'):
                query += ';'
            return self.execute_cmd('SPARQL ' + query)

    def __init__(self, url: str, default_graph: str) -> None:
        super().__init__(url, default_graph)

    def execute(
        self, query: str, spy: Spy, force_order: bool = False, timeout: int = 0
    ) -> Dict:
        if force_order:
            query = f'DEFINE sql:select-option "order" {query}'

        projection = set(re.findall('\\?[A-z0-9]+', query))
        triples = query.split(' .\n')
        if 't_direction 1' in triples[-2]:
            s, p, o, option = triples[-2].split(' ', 3)
            triples[-2] = f'{s} {p} ?v56 {option} .'
            triples[-2] += f'\n\tFILTER (?v56 = {o}) .'
            if p == '<http://www.wikidata.org/prop/direct/P279>':
                triples[-2] += '\n\t?v56 <http://www.wikidata.org/prop/direct/P279> ?v666'
            else:
                triples[-2] += '\n\t?v56 <http://www.wikidata.org/prop/direct/P31> ?v666'
            query = ' .\n'.join(triples)
            query = query.replace('*', ' '.join(projection))

        logging.debug('###' * 50)
        logging.debug(query)
        logging.debug('###' * 50 + '\n')

        sparql = SPARQLWrapper(self.url)
        sparql.setQuery(query)
        sparql.addDefaultGraph(self.default_graph)
        sparql.addParameter('timeout', str(timeout))
        sparql.setReturnFormat(JSON)

        solutions = None
        start_time = time.time()
        try:
            tentative = 1
            while True:
                try:
                    solutions = sparql.queryAndConvert()
                    break
                except Exception as error:
                    logging.error(f'attempt n° {tentative}')
                    if tentative >= 10 or "Connection refused" not in str(error):
                        raise error
                    tentative += 1
            elapsed_time = time.time() - start_time
            spy.report('', 'num_solutions', len(solutions['results']['bindings']))
            spy.report('', 'execution_time', elapsed_time)
            if timeout > 0 and (elapsed_time * 1000) > timeout:
                spy.report('', 'status', 'timeout')
            else:
                spy.report('', 'status', 'ok')
        except Exception as error:
            elapsed_time = time.time() - start_time
            logging.error(error)
            spy.report('', 'num_solutions', 0)
            spy.report('', 'execution_time', elapsed_time)
            if timeout > 0 and (elapsed_time * 1000) > timeout:
                spy.report('', 'status', 'timeout')
            else:
                spy.report('', 'status', 'error')
        return solutions

    def cost(self, query: str, force_order: bool = False) -> float:
        if force_order:
            query = f'DEFINE sql:select-option "order" {query}'
        isql = self.ISQLWrapper('localhost:1111', 'dba', 'dba')
        cmd = f"select explain('sparql {query}', -7);"
        response = isql.execute_cmd(cmd)
        return float(response.split('\n')[9].replace(',', '.'))


class Blazegraph(Endpoint):

    def __init__(self, url: str, default_graph: str) -> None:
        super().__init__(url, default_graph)

    def execute(
        self, query: str, spy: Spy, force_order: bool = False, timeout: int = 0
    ) -> Dict:
        if force_order:
            select, where = query.split('WHERE {')
            pragma = 'hint:Query hint:optimizer "None".'
            query = f'{select} WHERE {{\n\t{pragma} {where}'

        logging.debug('###' * 50)
        logging.debug(query)
        logging.debug('###' * 50 + '\n')

        sparql = SPARQLWrapper(self.url)
        sparql.setQuery(query)
        sparql.addDefaultGraph(self.default_graph)
        sparql.addParameter('timeout', str(timeout))
        sparql.setReturnFormat(JSON)

        solutions = None
        start_time = time.time()
        try:
            solutions = sparql.queryAndConvert()
            spy.report('', 'status', 'ok')
            spy.report('', 'num_solutions', len(solutions['results']['bindings']))
        except Exception as error:
            logging.error(error)
            spy.report('', 'status', 'error')
            spy.report('', 'num_solutions', 0)
        finally:
            elapsed_time = time.time() - start_time
            spy.report('', 'execution_time', elapsed_time)
            if timeout > 0 and elapsed_time > timeout:
                spy.report('', 'status', 'timeout')
            return solutions
