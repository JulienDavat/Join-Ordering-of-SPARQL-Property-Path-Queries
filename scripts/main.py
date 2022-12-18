import click
import time
import glob
import json
import utils
import logging
import os

from spy import Spy
from join_order import JoinOrder
from endpoint import Virtuoso, Blazegraph
from search import DummySearch, GreedySearch, HGreedySearch, DPSearch
from estimators.exact_count import ExactCountEstimator
from estimators.random_walks import RandomWalksEstimator
from estimators.void import VoidEstimator
from hdt_connector import HDTConnector
from typing import Optional, List


def summarize(join_order: JoinOrder) -> None:
    spy = Spy()
    fifo = join_order.root.children
    while len(fifo) > 0:
        node = fifo.pop(0)
        spy.report(node.k0, 'num_joins', node.size - 1)
        spy.report(node.k0, 'cardinality', node.cardinality)
        spy.report(node.k0, 'epsilon', node.epsilon)
        spy.report(node.k0, 'cost', node.cost)
        spy.report(node.k0, 'support', node.support)
        spy.report(node.k0, 'estimation_time', node.estimation_time)
        spy.report(node.k0, 'selected', False)
        for child in node.children:
            fifo.append(child)
    for node in join_order.decompose():
        spy.report(node.k0, 'selected', True)
    return spy


def initialize_logging(verbose: bool = False, logfile: Optional[str] = None):
    if logfile is None:
        handlers = [logging.StreamHandler()]
    else:
        handlers = [logging.FileHandler(logfile)]
    level = 'DEBUG' if verbose else 'INFO'
    logging.basicConfig(level=level, format='', handlers=handlers)


def list_files(path: str) -> List[str]:
    files = list()
    if os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.endswith('.sparql'):
                files.append(f'{path}/{filename}')
    else:
        files.extend(glob.glob(path))
    return files


@click.group()
def cli():
    pass


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--graph', type=click.STRING, default='wdbench')
@click.option('--num-walks', type=click.INT, default=10000)
@click.option('--max-depth', type=click.INT, default=5)
@click.option('--optimize-walk-plans', type=click.BOOL, default=True)
@click.option('--verbose/--quiet', default=False)
def estimate(path, graph, num_walks, max_depth, optimize_walk_plans, verbose):
    initialize_logging(verbose)
    connector = HDTConnector(graph)
    estimator = RandomWalksEstimator(
        connector, num_walks=num_walks, max_depth=max_depth,
        relaxe_stars=False, optimize_walk_plans=optimize_walk_plans)
    query = utils.parse_file(glob.glob(path)[0])
    join_order = DummySearch(estimator).run(query)
    start = time.time()
    estimator.estimate(join_order)
    elapsed_time = time.time() - start
    logging.info('===' * 50)
    logging.info(f'cardinality: {join_order.cardinality} +/- {join_order.epsilon}')
    logging.info(f'support: {join_order.support}')
    logging.info('---' * 50)
    logging.info(f'time: {elapsed_time} seconds')
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--verbose/--quiet', default=False)
@click.option('--output', type=click.Path(exists=True, dir_okay=True, file_okay=False))
def groundtruth_estimate(
    path, url, graph, verbose, output
):
    initialize_logging(verbose)
    endpoint = Virtuoso(url, graph)
    connector = HDTConnector(graph.split('/')[-1])
    estimator = VoidEstimator(connector, relaxe_stars=True)
    query = utils.parse_file(glob.glob(path)[0])
    join_order = DPSearch(estimator).run(query)
    spy = Spy()
    cardinality = endpoint.count(
        join_order.stringify('virtuoso'),
        spy,
        force_order=True,
        distinct=True)
    start = time.time()
    elapsed_time = time.time() - start
    logging.info('===' * 50)
    logging.info(f'cardinality: {cardinality}')
    logging.info('---' * 50)
    logging.info(f'time: {elapsed_time} seconds')
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.argument('target', type=click.Choice(['virtuoso', 'blazegraph']))
@click.option('--graph', type=click.STRING, default='wdbench')
@click.option('--estimator', type=click.Choice(['random-walks', 'void']), default='random-walks')
@click.option('--optimizer', type=click.Choice(['greedy', 'hgreedy', 'dp']), default='greedy')
@click.option('--num-walks', type=click.INT, default=10000)
@click.option('--max-depth', type=click.INT, default=5)
@click.option('--relaxe-stars', type=click.BOOL, default=True)
@click.option('--optimize-walk-plans', type=click.BOOL, default=True)
@click.option('--beam-size', type=click.INT, default=1)
@click.option('--beam-extra', type=click.INT, default=1)
@click.option('--verbose/--quiet', default=False)
@click.option('--output', type=click.Path(exists=True, dir_okay=True, file_okay=False))
def optimize(
    path, target, graph, estimator, optimizer, num_walks, max_depth, relaxe_stars,
    optimize_walk_plans, beam_size, beam_extra, verbose, output
):
    initialize_logging(verbose)
    connector = HDTConnector(graph)
    if estimator == 'random-walks':
        estimator = RandomWalksEstimator(
            connector, num_walks=num_walks, max_depth=max_depth,
            relaxe_stars=relaxe_stars, optimize_walk_plans=optimize_walk_plans)
    else:
        estimator = VoidEstimator(connector)
    if optimizer == 'greedy':
        optimizer = GreedySearch(estimator, beam_size=beam_size)
    elif optimizer == 'hgreedy':
        optimizer = HGreedySearch(
            estimator, beam_size=beam_size, beam_extra=beam_extra)
    else:
        optimizer = DPSearch(estimator)
    query = utils.parse_file(glob.glob(path)[0])
    start = time.time()
    join_order = optimizer.run(query)
    elapsed_time = time.time() - start
    spy1 = Spy()
    spy1.report('', 'optimization_time', elapsed_time)
    spy1.report('', 'cost', join_order.cost)
    spy1.report('', 'support', join_order.support)
    spy1.report('', 'cardinality', join_order.cardinality)
    spy1.report('', 'epsilon', join_order.epsilon)
    spy2 = summarize(join_order)
    if output is not None:
        with open(f'{output}/query.sparql', 'w') as writer:
            writer.write(join_order.stringify(target))
        spy1.to_csv(f'{output}/metrics.csv')
        spy2.to_csv(f'{output}/summary.csv')
    logging.info('===' * 50)
    logging.info(spy2.to_dataframe())
    logging.info('---' * 50)
    logging.info(join_order.stringify(target))
    logging.info('---' * 50)
    logging.info(spy1.to_dataframe())
    logging.info('---' * 50)
    logging.info(f'Query optimized in {elapsed_time} seconds')
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.argument('target', type=click.Choice(['virtuoso', 'blazegraph']))
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--timeout', type=click.INT, default=0)
@click.option('--relaxe-stars', type=click.BOOL, default=True)
@click.option('--optimizer', type=click.Choice(['greedy', 'hgreedy', 'dp']), default='greedy')
@click.option('--beam-size', type=click.INT, default=1)
@click.option('--beam-extra', type=click.INT, default=1)
@click.option('--verbose/--quiet', default=False)
@click.option('--output', type=click.Path(exists=True, dir_okay=True, file_okay=False))
def groundtruth_optimize(
    path, target, url, graph, timeout, relaxe_stars, optimizer,
    beam_size, beam_extra, verbose, output
):
    initialize_logging(verbose)
    endpoint = Virtuoso(url, graph)
    estimator = ExactCountEstimator(
        endpoint, timeout=timeout, relaxe_stars=relaxe_stars)
    if optimizer == 'greedy':
        optimizer = GreedySearch(estimator, beam_size=beam_size)
    elif optimizer == 'hgreedy':
        optimizer = HGreedySearch(
            estimator, beam_size=beam_size, beam_extra=beam_extra)
    else:
        optimizer = DPSearch(estimator)
    query = utils.parse_file(glob.glob(path)[0])
    start = time.time()
    join_order = optimizer.run(query)
    elapsed_time = time.time() - start
    spy = Spy()
    spy.report('', 'optimization_time', elapsed_time)
    spy.report('', 'cost', join_order.cost)
    spy.report('', 'support', join_order.support)
    spy.report('', 'cardinality', join_order.cardinality)
    spy.report('', 'epsilon', join_order.epsilon)
    if output is not None:
        with open(f'{output}/query.sparql', 'w') as writer:
            writer.write(join_order.stringify(target))
        spy.to_csv(f'{output}/metrics.csv')
    logging.info('===' * 50)
    logging.info(join_order.stringify(target))
    logging.info('---' * 50)
    logging.info(spy.to_dataframe())
    logging.info('---' * 50)
    logging.info(f'Query optimized in {elapsed_time} seconds')
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--timeout', type=click.INT, default=0)
@click.option('--force-order/--free-order', default=False)
@click.option('--verbose/--quiet', default=False)
@click.option('--results', type=click.Path(exists=False), default=None)
@click.option('--metrics', type=click.Path(exists=False), default=None)
def virtuoso_run(path, url, graph, force_order, timeout, verbose, results, metrics):
    initialize_logging(verbose)
    spy = Spy()
    virtuoso = Virtuoso(url, graph)
    query_file = glob.glob(path)[0]
    with open(query_file, 'r') as reader:
        query = reader.read()
    solutions = virtuoso.execute(query, spy, force_order=force_order, timeout=timeout)
    if solutions is not None and results is not None:
        with open(results, 'w') as writer:
            json.dump(solutions, writer, indent=2)
    if metrics is not None:
        spy.to_csv(metrics)
    logging.info('===' * 50)
    logging.info(spy.to_dataframe())
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--distinct', type=click.BOOL, default=False)
@click.option('--force-order/--free-order', default=False)
@click.option('--verbose/--quiet', default=False)
def virtuoso_count(path, url, graph, distinct, force_order, verbose):
    initialize_logging(verbose)
    spy = Spy()
    virtuoso = Virtuoso(url, graph)
    query_file = glob.glob(path)[0]
    with open(query_file, 'r') as reader:
        query = reader.read()
    virtuoso.count(query, spy, distinct=distinct, force_order=force_order)
    logging.info('===' * 50)
    logging.info(spy.to_dataframe())
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--distinct', type=click.BOOL, default=False)
@click.option('--force-order/--free-order', default=False)
@click.option('--output', type=click.Path(exists=False), default=None)
@click.option('--verbose/--quiet', default=False)
def virtuoso_cost(path, url, graph, distinct, force_order, output, verbose):
    initialize_logging(verbose)
    spy = Spy()
    virtuoso = Virtuoso(url, graph)
    query_file = glob.glob(path)[0]
    with open(query_file, 'r') as reader:
        query = reader.read()
    start_time = time.time()
    cost = virtuoso.cost(query, force_order=force_order)
    elapsed_time = time.time() - start_time
    spy.report('', 'cost', cost)
    spy.report('', 'optimization_time', elapsed_time)
    if output is not None:
        spy.to_csv(output)
    logging.info('===' * 50)
    logging.info(spy.to_dataframe())
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:9999/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--timeout', type=click.INT, default=0)
@click.option('--force-order/--free-order', default=False)
@click.option('--verbose/--quiet', default=False)
@click.option('--results', type=click.Path(exists=False), default=None)
@click.option('--metrics', type=click.Path(exists=False), default=None)
def blazegraph_run(path, url, graph, force_order, timeout, verbose, results, metrics):
    initialize_logging(verbose)
    spy = Spy()
    blazegraph = Blazegraph(url, graph)
    query_file = glob.glob(path)[0]
    with open(query_file, 'r') as reader:
        query = reader.read()
    solutions = blazegraph.execute(query, spy, force_order=force_order, timeout=timeout)
    if solutions is not None and results is not None:
        with open(results, 'w') as writer:
            json.dump(solutions, writer, indent=2)
    if metrics is not None:
        spy.to_csv(metrics)
    logging.info('===' * 50)
    logging.info(spy.to_dataframe())
    logging.info('===' * 50)


@cli.command()
@click.argument('path', type=click.STRING)
@click.option('--url', type=click.STRING, default='http://localhost:9999/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--distinct', type=click.BOOL, default=False)
@click.option('--force-order/--free-order', default=False)
@click.option('--verbose/--quiet', default=False)
def blazegraph_count(path, url, graph, distinct, force_order, verbose):
    initialize_logging(verbose)
    spy = Spy()
    blazegraph = Blazegraph(url, graph)
    query_file = glob.glob(path)[0]
    with open(query_file, 'r') as reader:
        query = reader.read()
    blazegraph.count(query, spy, distinct=distinct, force_order=force_order)
    logging.info('===' * 50)
    logging.info(spy.to_dataframe())
    logging.info('===' * 50)


@cli.command()
@click.argument('workload', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.option('--url', type=click.STRING, default='http://localhost:8890/sparql')
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--verbose/--quiet', default=False)
@click.option('--metrics', type=click.Path(exists=False), default=None)
def workload_statistics(workload, url, graph, verbose, metrics):
    initialize_logging(verbose)
    spy = Spy()
    virtuoso = Virtuoso(url, graph)
    for index, filename in enumerate(os.listdir(workload)):
        query = utils.parse_file(f'{workload}/{filename}')
        num_filters = len(query.filters)
        num_triple_patterns = 0
        num_path_patterns = 0
        num_constants = 0
        join_variables = set()
        for pattern in query.patterns:
            if pattern.more:
                num_path_patterns += 1
            else:
                num_triple_patterns += 1
            if pattern.subject[0] != '?':
                num_constants += 1
            if pattern.object[0] != '?':
                num_constants += 1
            variables = set()
            for other in query.patterns:
                if pattern == other:
                    continue
                variables.update(other.variables)
            join_variables.update(pattern.variables.intersection(variables))
        num_join_variables = len(join_variables)
        num_solutions = virtuoso.count(str(query), Spy(), distinct=True)
        spy.report(query.name, 'query', query.name)
        spy.report(query.name, 'num_filters', num_filters)
        spy.report(query.name, 'num_triple_patterns', num_triple_patterns)
        spy.report(query.name, 'num_path_patterns', num_path_patterns)
        spy.report(query.name, 'num_join_variables', num_join_variables)
        spy.report(query.name, 'num_constants', num_constants)
        spy.report(query.name, 'num_solutions', num_solutions)
        if index > 0:
            logging.debug('---' * 50)
        logging.debug(f'query: {query.name}')
        logging.debug(f'number of filters: {num_filters}')
        logging.debug(f'number of triple patterns: {num_triple_patterns}')
        logging.debug(f'number of path patterns: {num_path_patterns}')
        logging.debug(f'number of join variables: {num_join_variables}')
        logging.debug(f'number of constants: {num_constants}')
        logging.debug(f'number of solutions: {num_solutions}')
    if metrics is not None:
        spy.to_csv(metrics)
    print(spy.to_dataframe())


@cli.command()
@click.argument('path', type=click.STRING)
@click.argument('target', type=click.Choice(['virtuoso', 'blazegraph']))
@click.option('--verbose/--quiet', default=False)
@click.option('--output', type=click.Path(exists=False), default=None)
def prepare_query(path, target, verbose, output):
    initialize_logging(verbose)
    query = utils.parse_file(glob.glob(path)[0])
    if output is not None:
        with open(output, 'w') as writer:
            writer.write(query.stringify(target))
    logging.info('===' * 50)
    logging.info(query.stringify(target))
    logging.info('===' * 50)


@cli.command()
@click.argument('subject', type=click.STRING)
@click.argument('predicate', type=click.STRING)
@click.argument('object', type=click.STRING)
@click.option('--graph', type=click.STRING, default='http://example.com/wdbench')
@click.option('--limit', type=click.INT, default=0)
def scan(subject, predicate, object, graph, limit):
    connector = HDTConnector(graph)
    iterator = connector.create_iterator(subject, predicate, object)
    offset = 0
    while iterator.next() and (limit == 0 or offset < limit):
        if offset > 0:
            print('---' * 50)
        print(f'subject: {iterator.subject()}')
        print(f'predicate: {iterator.predicate()}')
        print(f'object: {iterator.object()}')
        offset = iterator.get_offset


if __name__ == '__main__':
    cli()
