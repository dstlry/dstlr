#!/usr/bin/env bash

spark-submit --class io.dstlr.LoadTriples \
        --num-executors 1 --executor-cores 1 \
        --driver-memory 8G --executor-memory 8G \
        --conf spark.executor.heartbeatInterval=10000 \
        target/scala-2.11/dstlr-assembly-0.1.jar \
        --input triples --neo4j.password password --neo4j.uri bolt://localhost:7687 --neo4j.batch.size 10000
