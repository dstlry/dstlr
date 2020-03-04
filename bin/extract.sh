#!/usr/bin/env bash

spark-submit --class io.dstlr.ExtractTriples \
        --num-executors 32 --executor-cores 8 \
        --driver-memory 64G --executor-memory 48G \
        --conf spark.executor.heartbeatInterval=10000 \
        --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-9-openjdk-amd64 \
        target/scala-2.11/dstlr-assembly-0.1.jar \
        --solr.uri localhost:9983 --solr.index core18 --max_rows 10 --query *:* --partitions 2048 --output triples --sent-length-threshold 256
