import os
import pandas
import itertools


def list_queries(workload):
    files = []
    for filename in os.listdir(f"queries/{workload}"):
        if filename.endswith(".sparql"):
            files.append(filename.split(".")[0])
    return files


def metrics_todo(wcs):
    files = []
    for xp in config["experiments"]:
        if "runonly" not in config or xp in config["runonly"]:
            workload = config["experiments"][xp]["workload"]
            estimator = config["experiments"][xp]["estimator"]
            for query in list_queries(workload):
                for run in config["runs"]:
                    if estimator == "baseline":
                        files.append(f"output/{workload}/baseline/{xp}/{query}.{run}.csv")
                    elif estimator == "exact-count":
                        files.append(f"output/{workload}/groundtruth/{xp}/{query}.{run}.csv")
                    else:
                        files.append(f"output/{workload}/experiments/{xp}/{query}.{run}.csv")
    return files


def summaries_todo(wcs):
    files = []
    for xp in config["experiments"]:
        if "runonly" not in config or xp in config["runonly"]:
            workload = config["experiments"][xp]["workload"]
            estimator = config["experiments"][xp]["estimator"]
            if estimator == "baseline" or estimator == "exact-count":
                continue
            for query in list_queries(workload):
                for run in config["runs"]:
                    files.append(f"output/{workload}/experiments/{xp}/{query}.summary.csv")
    return files


def statistics_todo(wcs):
    files = []
    workloads = [config["experiments"][xp]["workload"] for xp in config["experiments"]]
    for workload in workloads:
        files.append(f"output/{workload}/statistics.csv")
    return files


def get_endpoint_url(wcs):
    endpoint = config["experiments"][wcs.xp]["endpoint"]
    return config["endpoints"][endpoint]


def get_endpoint_graph(wcs):
    endpoint = config["experiments"][wcs.xp]["endpoint"]
    return config["graphs"][wcs.workload][endpoint]


def get_timeout(wcs):
    if "timeout" not in config:
        return 0
    return int(config["timeout"])


wildcard_constraints:
    query = "[A-z0-9\\-]+",
    xp = "xp([0-9]|-)+",
    workload = "(wdbench|wdbench-ppaths|wdbench-stars|jobrdf|test)",
    run = "[1-9]"


include: "rules/baseline.smk"
include: "rules/experiments.smk"
include: "rules/statistics.smk"
include: "rules/groundtruth.smk"


rule all:
    input: ["output/metrics.csv", "output/summaries.csv", "output/statistics.csv"]
    priority: 10


rule metrics:
    input: ancient(metrics_todo)
    output: "output/metrics.csv"
    priority: 10
    run:
        dfs = [pandas.read_csv(file) for file in input]
        pandas.concat(dfs).to_csv(str(output), index=False)


rule summaries:
    input: ancient(summaries_todo)
    output: "output/summaries.csv"
    priority: 10
    run:
        dfs = [pandas.read_csv(file) for file in input]
        pandas.concat(dfs).to_csv(str(output), index=False)


rule statistics:
    input: ancient(statistics_todo)
    output: "output/statistics.csv"
    priority: 10
    run:
        dfs = [pandas.read_csv(file) for file in input]
        pandas.concat(dfs).to_csv(str(output), index=False)
