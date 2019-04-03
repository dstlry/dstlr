#!/bin/bash

# Start Neo4j
docker run -d --publish=7474:7474 --publish=7687:7687 --volume=$NEO4J_DIR:/data -e NEO4J_dbms_memory_heap_max__size=32G -e NEO4J_dbms_memory_heap_initial__size=32G -e NEO4J_dbms_memory_pagecache_size=2G neo4j