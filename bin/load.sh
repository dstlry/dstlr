#!/usr/bin/env bash

spark-submit --class io.dstlr.LoadTriples \
        --num-executors 1 --executor-cores 1 \
        --driver-memory 16G --executor-memory 16G \
        --conf spark.executor.heartbeatInterval=60 \
        target/scala-2.11/dstlr-assembly-0.1.jar \
        --input triples-5000d-128s --neo4j.password password --neo4j.uri bolt://192.168.1.110:7687 --neo4j.batch.size 10000