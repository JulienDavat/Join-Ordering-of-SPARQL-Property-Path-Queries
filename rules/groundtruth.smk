rule groundtruth_post_process:
    input:
        optimization = "output/{workload}/groundtruth/{xp}/{query}/metrics.csv",
        execution = "output/{workload}/groundtruth/{xp}/{query}/{run}.csv"
    output:
        "output/{workload}/groundtruth/{xp}/{query}.{run}.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        optimizer = (lambda wcs: config["experiments"][wcs.xp]["optimizer"]),
        beam_size = (lambda wcs: config["experiments"][wcs.xp]["beam_size"]),
        beam_extra = (lambda wcs: config["experiments"][wcs.xp]["beam_extra"]),
        relaxe_stars = (lambda wcs: config["experiments"][wcs.xp]["relaxe_stars"])
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
        if "estimator" not in df:
            df["estimator"] = "exact-count"
        if "optimizer" not in df:
            df["optimizer"] = params.optimizer
        if "beam_size" not in df:
            df["beam_size"] = params.beam_size
        if "beam_extra" not in df:
            df["beam_extra"] = params.beam_extra
        if "relaxe_stars" not in df:
            df["relaxe_stars"] = params.relaxe_stars
        df.to_csv(str(output), index=False)


rule groundtruth_optimize:
    input:
        "queries/{workload}/{query}.sparql"
    output:
        query = "output/{workload}/groundtruth/{xp}/{query}/query.sparql",
        metrics = "output/{workload}/groundtruth/{xp}/{query}/metrics.csv"
    params:
        url = (lambda wcs: get_endpoint_url(wcs)),
        graph = (lambda wcs: get_endpoint_graph(wcs)),
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        timeout = (lambda wcs: config["experiments"][wcs.xp]["timeout"]),
        relaxe_stars = (lambda wcs: config["experiments"][wcs.xp]["relaxe_stars"]),
        optimizer = (lambda wcs: config["experiments"][wcs.xp]["optimizer"]),
        beam_size = (lambda wcs: config["experiments"][wcs.xp]["beam_size"]),
        beam_extra = (lambda wcs: config["experiments"][wcs.xp]["beam_extra"])
    priority: 10
    run:
        shell("bash server.sh start virtuoso")
        shell("mkdir -p output/{wildcards.workload}/groundtruth/{wildcards.xp}/{wildcards.query}")
        shell("python scripts/main.py groundtruth {input} {params.endpoint} \
            --url {params.url} \
            --graph {params.graph} \
            --timeout {params.timeout} \
            --relaxe-stars {params.relaxe_stars} \
            --estimator {params.estimator} \
            --beam-size {params.beam_size} \
            --beam-extra {params.beam_extra} \
            --output output/{wildcards.workload}/groundtruth/{wildcards.xp}/{wildcards.query}")
        shell("bash server.sh stop virtuoso")


rule groundtruth_first_exec:
    input:
        "output/{workload}/groundtruth/{xp}/{query}/query.sparql"
    output:
        "output/{workload}/groundtruth/{xp}/{query}/1.csv"
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


rule groundtruth_second_exec:
    input:
        "output/{workload}/groundtruth/{xp}/{query}/query.sparql"
    output:
        "output/{workload}/groundtruth/{xp}/{query}/2.csv"
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


rule groundtruth_third_exec:
    input:
        "output/{workload}/groundtruth/{xp}/{query}/query.sparql"
    output:
        "output/{workload}/groundtruth/{xp}/{query}/3.csv"
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


rule groundtruth_fourth_exec:
    input:
        "output/{workload}/groundtruth/{xp}/{query}/query.sparql"
    output:
        "output/{workload}/groundtruth/{xp}/{query}/4.csv"
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


rule groundtruth_fifth_exec:
    input:
        "output/{workload}/groundtruth/{xp}/{query}/query.sparql"
    output:
        "output/{workload}/groundtruth/{xp}/{query}/5.csv"
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
