# dstlr

# Setup

```bash
docker run -d --publish=7474:7474 --publish=7687:7687 \
    --volume=`pwd`/neo4j:/data \
    -e NEO4J_dbms_memory_pagecache_size=2G \
    -e NEO4J_dbms_memory_heap_initial__size=4G \
    -e NEO4J_dbms_memory_heap_max__size=16G \
    neo4j
```