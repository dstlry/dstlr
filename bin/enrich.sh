#!/usr/bin/env bash

spark-submit --class io.dstlr.EnrichTriples \
        --num-executors 1 --executor-cores 1 \
        --driver-memory 8G --executor-memory 8G \
        --conf spark.executor.heartbeatInterval=10000 \
        ../target/scala-2.11/dstlr-assembly-0.1.jar --input triples --output triples-enriched --partitions 1
