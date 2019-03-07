#!/bin/bash

spark-submit --driver-java-options="-Djava.rmi.server.hostname=129.97.186.18 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1098" --class io.dstlr.Spark --num-executors 44 --executor-cores 8 --driver-memory 64G target/scala-2.11/dstlr-assembly-0.1.jar --solr.url localhost:9990 --neo4j.password password --search.term music --partitions 44
