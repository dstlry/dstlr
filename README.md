# dstlr

`dstlr` is a system for large-scale knowledge extraction using [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), [Apache Spark](https://spark.apache.org/), and [neo4j](https://neo4j.com/). It takes a (potentially large) collection of text documents and horiztonally scales out CoreNLP via Spark to extract mentions of named entities, the relations between them, and links to an entity in a knowledge base. From this, we generate a knowledge graph from the unstructured text for which we can pose interesting queries via neo4j's Cypher query language. We show a number of interesting uses cases for data cleaning.

# Setup

## neo4j

If don't already have a neo4j instance running, you can start one via Docker with the command (after updating heap size params):
```bash
docker run -d --publish=7474:7474 --publish=7687:7687 \
    --volume=`pwd`/neo4j:/data \
    -e NEO4J_dbms_memory_pagecache_size=2G \
    -e NEO4J_dbms_memory_heap_initial__size=4G \
    -e NEO4J_dbms_memory_heap_max__size=16G \
    neo4j
```

In order for efficient inserts and queries, build the following indexes in neo4j:
```
CREATE INDEX ON :Document(id)
CREATE INDEX ON :Entity(id)
CREATE INDEX ON :Fact(relation)
CREATE INDEX ON :Fact(value)
CREATE INDEX ON :Fact(relation, value)
CREATE INDEX ON :Mention(id)
CREATE INDEX ON :Mention(class)
CREATE INDEX ON :Mention(index)
CREATE INDEX ON :Mention(span)
CREATE INDEX ON :Mention(id, class, span)
CREATE INDEX ON :Relation(type)
```

## Data Cleaning Queries

Find CITY_OF_HEADQUARTERS relation between two mentions:
```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
RETURN d, s, r, o, e, f
LIMIT 25
```

Find CITY_OF_HEADQUARTERS relation between two mentions where the subject node doesn't have a linked entity:
```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
OPTIONAL MATCH (s)-->(e:Entity)
WHERE e IS NULL
RETURN d, s, r, o, e
LIMIT 25
```

Find CITY_OF_HEADQUARTERS relation between two mentions where the linked entity doesn't have the relation we're looking for:
```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity)
OPTIONAL MATCH (e)-->(f:Fact {relation: r.type})
WHERE f IS NULL
RETURN d, s, r, o, e, f
LIMIT 25
```
