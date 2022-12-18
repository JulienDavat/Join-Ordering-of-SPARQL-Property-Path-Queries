rule experiments_post_process_metrics:
    input:
        optimization = ancient("output/{workload}/experiments/{xp}/{query}/metrics.csv"),
        execution = ancient("output/{workload}/experiments/{xp}/{query}/{run}.csv")
    output:
        "output/{workload}/experiments/{xp}/{query}.{run}.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        estimator = (lambda wcs: config["experiments"][wcs.xp]["estimator"]),
        optimizer = (lambda wcs: config["experiments"][wcs.xp]["optimizer"]),
        num_walks = (lambda wcs: config["experiments"][wcs.xp]["num_walks"]),
        max_depth = (lambda wcs: config["experiments"][wcs.xp]["max_depth"]),
        beam_size = (lambda wcs: config["experiments"][wcs.xp]["beam_size"]),
        beam_extra = (lambda wcs: config["experiments"][wcs.xp]["beam_extra"]),
        relaxe_stars = (lambda wcs: config["experiments"][wcs.xp]["relaxe_stars"]),
        optimize_walk_plans = (lambda wcs: config["experiments"][wcs.xp]["optimize_walk_plans"])
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
            df["estimator"] = params.estimator
        if "optimizer" not in df:
            df["optimizer"] = params.optimizer
        if "num_walks" not in df:
            df["num_walks"] = params.num_walks
        if "max_depth" not in df:
            df["max_depth"] = params.max_depth
        if "beam_size" not in df:
            df["beam_size"] = params.beam_size
        if "beam_extra" not in df:
            df["beam_extra"] = params.beam_extra
        if "relaxe_stars" not in df:
            df["relaxe_stars"] = params.relaxe_stars
        if "optimize_walk_plans" not in df:
            df["optimize_walk_plans"] = params.optimize_walk_plans
        df.to_csv(str(output), index=False)


rule experiments_post_process_summary:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/summary.csv")
    output:
        "output/{workload}/experiments/{xp}/{query}.summary.csv"
    params:
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        estimator = (lambda wcs: config["experiments"][wcs.xp]["estimator"]),
        optimizer = (lambda wcs: config["experiments"][wcs.xp]["optimizer"]),
        num_walks = (lambda wcs: config["experiments"][wcs.xp]["num_walks"]),
        max_depth = (lambda wcs: config["experiments"][wcs.xp]["max_depth"]),
        beam_size = (lambda wcs: config["experiments"][wcs.xp]["beam_size"]),
        beam_extra = (lambda wcs: config["experiments"][wcs.xp]["beam_extra"]),
        relaxe_stars = (lambda wcs: config["experiments"][wcs.xp]["relaxe_stars"]),
        optimize_walk_plans = (lambda wcs: config["experiments"][wcs.xp]["optimize_walk_plans"])
    priority: 10
    run:
        df = pandas.read_csv(str(input))
        if "workload" not in df:
            df["workload"] = wildcards.workload
        if "xp" not in df:
            df["xp"] = wildcards.xp
        if "query" not in df:
            df["query"] = wildcards.query
        if "endpoint" not in df:
            df["endpoint"] = params.endpoint
        if "estimator" not in df:
            df["estimator"] = params.estimator
        if "optimizer" not in df:
            df["optimizer"] = params.optimizer
        if "num_walks" not in df:
            df["num_walks"] = params.num_walks
        if "max_depth" not in df:
            df["max_depth"] = params.max_depth
        if "beam_size" not in df:
            df["beam_size"] = params.beam_size
        if "beam_extra" not in df:
            df["beam_extra"] = params.beam_extra
        if "relaxe_stars" not in df:
            df["relaxe_stars"] = params.relaxe_stars
        if "optimize_walk_plans" not in df:
            df["optimize_walk_plans"] = params.optimize_walk_plans
        df.to_csv(str(output), index=False)


rule experiments_optimize:
    input:
        ancient("queries/{workload}/{query}.sparql")
    output:
        query = "output/{workload}/experiments/{xp}/{query}/query.sparql",
        metrics = "output/{workload}/experiments/{xp}/{query}/metrics.csv",
        summary = "output/{workload}/experiments/{xp}/{query}/summary.csv"
    params:
        graph = (lambda wcs: config["graphs"][wcs.workload]["hdt"]),
        endpoint = (lambda wcs: config["experiments"][wcs.xp]["endpoint"]),
        estimator = (lambda wcs: config["experiments"][wcs.xp]["estimator"]),
        optimizer = (lambda wcs: config["experiments"][wcs.xp]["optimizer"]),
        num_walks = (lambda wcs: config["experiments"][wcs.xp]["num_walks"]),
        max_depth = (lambda wcs: config["experiments"][wcs.xp]["max_depth"]),
        beam_size = (lambda wcs: config["experiments"][wcs.xp]["beam_size"]),
        beam_extra = (lambda wcs: config["experiments"][wcs.xp]["beam_extra"]),
        relaxe_stars = (lambda wcs: config["experiments"][wcs.xp]["relaxe_stars"]),
        optimize_walk_plans = (lambda wcs: config["experiments"][wcs.xp]["optimize_walk_plans"])
    priority: 10
    run:
        shell("mkdir -p output/{wildcards.workload}/experiments/{wildcards.xp}/{wildcards.query}")
        shell("python scripts/main.py optimize {input} {params.endpoint} \
            --graph {params.graph} \
            --estimator {params.estimator} \
            --optimizer {params.optimizer} \
            --num-walks {params.num_walks} \
            --max-depth {params.max_depth} \
            --beam-size {params.beam_size} \
            --beam-extra {params.beam_extra} \
            --relaxe-stars {params.relaxe_stars} \
            --optimize-walk-plans {params.optimize_walk_plans} \
            --output output/{wildcards.workload}/experiments/{wildcards.xp}/{wildcards.query}")


rule experiments_first_exec:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/experiments/{xp}/{query}/1.csv"
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


rule experiments_second_exec:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/experiments/{xp}/{query}/2.csv"
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


rule experiments_third_exec:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/experiments/{xp}/{query}/3.csv"
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


rule experiments_fourth_exec:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/experiments/{xp}/{query}/4.csv"
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


rule experiments_fifth_exec:
    input:
        ancient("output/{workload}/experiments/{xp}/{query}/query.sparql")
    output:
        "output/{workload}/experiments/{xp}/{query}/5.csv"
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
