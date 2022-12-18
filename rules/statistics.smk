rule workload_post_process_statistics:
    input:
        "output/{workload}/statistics.tmp.csv"
    output:
        "output/{workload}/statistics.csv"
    priority: 10
    run:
        df = pandas.read_csv(str(input))
        if "workload" not in df:
            df["workload"] = wildcards.workload
        df.to_csv(str(output), index=False)


rule workload_statistics:
    output:
        "output/{workload}/statistics.tmp.csv"
    params:
        url = (lambda wcs: config["endpoints"]["virtuoso"]),
        graph = (lambda wcs: config["graphs"][wcs.workload]["virtuoso"])
    priority: 10
    run:
        shell("bash server.sh start virtuoso")
        shell("python scripts/main.py workload-statistics queries/{wildcards.workload} \
            --url {params.url} \
            --graph {params.graph} \
            --metrics {output}")
        shell("bash server.sh stop virtuoso")
