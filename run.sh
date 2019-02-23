#!/bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --no-browser --port 8181'
#export PYSPARK_PYTHON=/usr/bin/python3

export CORENLP_HOME="/tuna1/scratch/ryan/git/dstlr/corenlp"

pyspark --num-executors 44 --executor-cores 1 --executor-memory 4G --driver-memory 16G --jars spark-solr.jar
