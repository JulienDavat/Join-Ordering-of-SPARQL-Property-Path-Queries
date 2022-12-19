# Join Ordering of SPARQL Property Path Queries

This repository contains the source code, the configuration files, and the queries
used in the experimental study presented in our paper [Join Ordering of SPARQL Property Path Queries](...).

## Setup

To quickly get started, run the following commands on one machine, it will install everything you need to reproduce our experimental results.

1. Clone and install the project
    
    <details>
    <summary>Details</summary>
    <br>
    
    We use conda to manage the project dependencies. If conda is not installed
    on your system, you can download it from their [website](https://docs.conda.io/en/latest/miniconda.html).

    ```bash
    git clone ... xp-eswc2023
    cd xp-eswc2023

    conda env create -f environment.yml
    conda activate xp
    ```

    </details>
  
  
2. Install HDT
    
    <details>
    <summary>Details</summary>
    <br>
    
    In this project we use a custom version of HDT that need to be installed on your system.

    ```bash
    git clone https://github.com/JulienDavat/hdt-bindings.git hdt
    cd hdt

    git clone git@github.com:rdfhdt/hdt-cpp.git
    cd hdt-cpp
    git checkout tags/v1.3.3 -b master 
    cd ..
    
    python -m pip install .
    ```

    </details>
  
  
3. Download HDT files
    
    <details>
    <summary>Details</summary>
    <br>
    
    Random Walks are performed over HDT. Please download HDT files from this
    [link](https://drive.google.com/file/d/1QAIKOBn4CMPBiBMoTsiXC6NQTjtJh6VB/view?usp=sharing) into the [data](https://github.com/JulienDavat/Join-Ordering-of-SPARQL-Property-Path-Queries/tree/main/data) directory. If the *data* directory does not exist, please create it.

    </details>
    
    
4. Install Virtuoso v7.2.7
  
    <details>
    <summary>Details</summary>

    ```bash
    wget https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.7/virtuoso-opensource-7.2.7.tar.gz
    tar -zxvf virtuoso-opensource-7.2.7.tar.gz

    cd virtuoso-opensource-7.2.7
    ./configure
    make
    make install
    ```
    
    The configuration file used in our experiments is available in the [config](https://github.com/JulienDavat/Join-Ordering-of-SPARQL-Property-Path-Queries/tree/main/config)
    directory. You just have to indicate the location of Virtuoso on your system.
    The location of Virtuoso must also be reported in the [server.sh](https://github.com/JulienDavat/Join-Ordering-of-SPARQL-Property-Path-Queries/blob/main/server.sh) script. Finally,
    you need to add the *bin* directory of Virtuoso in your *PATH* variable.
    
    If everything went well, you should be able to start Virtuoso with the following
    command:
    
    ```bash
    bash server.sh start virtuoso
    ```
    
    Virtuoso can be stopped using the same command:
    
    ```bash
    bash server.sh stop virtuoso
    ```

    </details>
  
  
5. Install BlazeGraph v2.1.6
  
    <details>
    <summary>Details</summary>

    ```bash
    wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/bigdata.jar
    ```

    The configuration file used in our experiments is available in the [config](https://github.com/JulienDavat/Join-Ordering-of-SPARQL-Property-Path-Queries/tree/main/config)
    directory. You just have to copy it in the same directory as the .jar file. The
    location of BlazeGraph must be reported in the [server.sh](https://github.com/JulienDavat/Join-Ordering-of-SPARQL-Property-Path-Queries/blob/main/server.sh) script.
    
    If everything went well, you should be able to start BlazeGraph with the following
    command:
    
    ```bash
    bash server.sh start blazegraph
    ```
    
    BlazeGraph can be stopped using the same command:
    
    ```bash
    bash server.sh stop blazegraph
    ```

    </details>
  
  
6. Download the WDBench dataset.
    
    <details>
    <summary>Details</summary>
    <br>

    The dataset can be downloaded from [Figshare](https://figshare.com/s/50b7544ad6b1f51de060). If there is any problem, please refer to their
    official [github repository](https://github.com/MillenniumDB/WDBench). 

    </details>
  
  
7. Load data into Virtuoso

    <details>
    <summary>Details</summary>
    <br>
    
    The WDBench dataset can be loaded into Virtuoso using the following
    commands. You just have to indicate the location of the .nt file.

    ```bash
    isql "EXEC=ld_dir('<your file here>', '*.nt', 'http://example.com/wdbench');"
    isql "EXEC=rdf_loader_run();"
    isql "EXEC=checkpoint;"
    ```
  
    </details>
    
    
8. Load data into BlazeGraph

    <details>
    <summary>Details</summary>
    <br>
    
    The WDBench dataset can be loaded into BlazeGraph using the following
    command. You just have to indicate the location of the .nt file.

    ```bash
    java -cp blazegraph.jar com.bigdata.rdf.store.DataLoader -defaultGraph http://example.com/wdbench blazegraph.properties <your file here>
    ```

    </details>
    
    
## Quickstart

Experiments are powered by [snakemake](https://snakemake.readthedocs.io/en/stable), a scientific workflow management system in Python. To re-run our experiments just run
the following commands:

```bash
# For Virtuoso
snakemake --configfile virtuoso.yaml -C runs=[1,2,3,4] timeout=900000 -c1

# For BlazeGraph
snakemake --configfile blazegraph.yaml -C runs=[1,2,3,4] timeout=900 -c1
```

## Visualization

The data generated by the two snakemake commands are available in the
[output](...) directory. To visualize the data, you can use the provided
jupyter notebook. You just have to run the following command:

```bash
jupyter notebook eswc2023.ipynb
```