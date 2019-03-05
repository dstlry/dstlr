#!/bin/bash

###
# Configuration
###

# The Neo4j data directory for the Docker volume
NEO4J_DIR=neo4j

# The name of the Docker container
NEO4J_NAME=neo4j

# The number of Spark executors
SPARK_NUM_EXECUTORS=44

# The number of cores per Spark executor
SPARK_EXECUTOR_CORES=1

# The amount of memory per Spark executor
SPARK_EXECUTOR_MEMORY=8G

# The amount of memory for the Spark driver
SPARK_DRIVER_MEMORY=4G

# Activate venv
source venv/bin/activate

# The PySpark options for Jupyter
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --port 8181 --no-browser'
export PYSPARK_PYTHON=venv/bin/python

###
# Running
###

# Start Neo4j
docker run -d --publish=7474:7474 --publish=7687:7687 --volume=$NEO4J_DIR:/data -e NEO4J_dbms_memory_heap_max__size=32G -e NEO4J_dbms_memory_heap_initial__size=32G -e NEO4J_dbms_memory_pagecache_size=2G neo4j

# Install (or update) Apache Toree kernel
jupyter toree install --user --spark_opts="--num-executors $SPARK_NUM_EXECUTORS --executor-cores $SPARK_EXECUTOR_CORES --executor-memory $SPARK_EXECUTOR_MEMORY --driver-memory $SPARK_DRIVER_MEMORY"

# Start the Jupyter server using the PySpark driver
pyspark --num-executors $SPARK_NUM_EXECUTORS --executor-cores $SPARK_EXECUTOR_CORES --executor-memory $SPARK_EXECUTOR_MEMORY --driver-memory $SPARK_DRIVER_MEMORY --jars spark-solr.jar
