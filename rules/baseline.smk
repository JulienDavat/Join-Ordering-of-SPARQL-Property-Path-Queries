rule baseline_post_process:
    input:
        optimization = ancient("output/{workload}/baseline/{xp}/{query}/metrics.csv"),
        execution = ancient("output/{workload}/baseline/{xp}/{query}/{run}.csv")
    output:
        "output/{workload}/baseline/{xp}/{query}.{run}.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"])
    priority: 10
    run:
        df1 = pandas.read_csv(str(input.optimization))
        df2 = pandas.read_csv(str(input.execution))
        df = df1.merge(df2, how='cross', copy=False)
        if "workload" not in df:
            df["workload"] = wildcards.workload
        if "xp" not in df:
            df["xp"] = wildcards.xp
        if "query" not in df:
            df["query"] = wildcards.query
        if "run" not in df:
            df["run"] = wildcards.run
        if "endpoint" not in df:
            df["endpoint"] = params.endpoint
        df.to_csv(str(output), index=False)


rule baseline_prepare_query:
    input:
        ancient("queries/{workload}/{query}.sparql")
    output:
        query = "output/{workload}/baseline/{xp}/{query}/query.sparql",
        metrics = "output/{workload}/baseline/{xp}/{query}/metrics.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs))
    priority: 10
    run:
        shell("python scripts/main.py prepare-query {input} {params.endpoint} \
            --output {output.query}")
        if params.endpoint == "virtuoso":
            shell("python scripts/main.py virtuoso-cost {input} \
                --url {params.url} \
                --graph {params.graph} \
                --output {output.metrics}")
        else:
            df = panras.DataFrame()
            df["cost"] = 0.0
            df["optimization_time"] = 0.0
            df.to_csv(output.metrics, index=False)


rule baseline_first_exec:
    input:
        ancient("output/{workload}/baseline/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/baseline/{xp}/{query}/1.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        timeout = (lambda wcs: get_timeout(wcs))
    threads: workflow.cores
    priority: 5
    run:
        shell("bash server.sh start {params.endpoint}")
        shell("python scripts/main.py {params.endpoint}-run {input} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --metrics {output}")
        if "restartserver" in config and config["restartserver"]:
            shell("bash server.sh stop {params.endpoint}")


rule baseline_second_exec:
    input:
        ancient("output/{workload}/baseline/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/baseline/{xp}/{query}/2.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        timeout = (lambda wcs: get_timeout(wcs))
    threads: workflow.cores
    priority: 4
    run:
        shell("bash server.sh start {params.endpoint}")
        shell("python scripts/main.py {params.endpoint}-run {input} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --metrics {output}")
        if "restartserver" in config and config["restartserver"]:
            shell("bash server.sh stop {params.endpoint}")


rule baseline_third_exec:
    input:
        ancient("output/{workload}/baseline/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/baseline/{xp}/{query}/3.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        timeout = (lambda wcs: get_timeout(wcs))
    threads: workflow.cores
    priority: 3
    run:
        shell("bash server.sh start {params.endpoint}")
        shell("python scripts/main.py {params.endpoint}-run {input} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --metrics {output}")
        if "restartserver" in config and config["restartserver"]:
            shell("bash server.sh stop {params.endpoint}")


rule baseline_fourth_exec:
    input:
        ancient("output/{workload}/baseline/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/baseline/{xp}/{query}/4.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        timeout = (lambda wcs: get_timeout(wcs))
    threads: workflow.cores
    priority: 2
    run:
        shell("bash server.sh start {params.endpoint}")
        shell("python scripts/main.py {params.endpoint}-run {input} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --metrics {output}")
        if "restartserver" in config and config["restartserver"]:
            shell("bash server.sh stop {params.endpoint}")


rule baseline_fifth_exec:
    input:
        ancient("output/{workload}/baseline/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/baseline/{xp}/{query}/5.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        timeout = (lambda wcs: get_timeout(wcs))
    threads: workflow.cores
    priority: 1
    run:
        shell("bash server.sh start {params.endpoint}")
        shell("python scripts/main.py {params.endpoint}-run {input} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --metrics {output}")
        if "restartserver" in config and config["restartserver"]:
            shell("bash server.sh stop {params.endpoint}")
