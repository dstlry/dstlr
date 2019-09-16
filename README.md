# dstlr

`dstlr` is a system for large-scale knowledge extraction using [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), [Apache Spark](https://spark.apache.org/), and [neo4j](https://neo4j.com/). It takes a (potentially large) collection of unstructured text documents and horiztonally scales out CoreNLP via Spark to extract mentions of named entities, the relations between them, and links to an entity in an existing knowledge base. For relations of interest, we augment our extracted facts with corresponding ground-truth values from an existing knowledge base in order to reason about the quality of the text documents. From this, we generate a knowledge graph on which we can pose a number of queries via neo4j's Cypher query language to explore the text in a more structured manner.

We can discover a number of different scenarios relating facts asserted in documents to facts present in the knowledge base:
+ Supporting information - agreement between value in document and ground-truth from knowledge base
+ Inconsistent information - disagreement between value in document and ground-truth from knowledge base
+ Missing information - document contains information missing in knowledge base

Currently, we use Wikidata as a stand-in knowledge base as the source of ground-truth and extract relations from nearly 600,000 Washington Post news articles. We extract 5,405,447 relations and 27,004,318 entity mentions (linking to 324,094 Wikidata entities).

# Setup

## anserini

Download and build [Anserini](http://anserini.io) and then follow the [Solrini](https://github.com/castorini/anserini/blob/master/docs/solrini.md) instructions to get a Solr instance running for indexing text documents. Index a document collection with Anserini, such as the Washington Post collection, and ensure the appropriate Solr [command-line parameters](https://github.com/dstlry/dstlr/blob/master/src/main/scala/io/dstlr/package.scala) for `dstlr` are adjusted if use non-default options.

## neo4j

Start a neo4j instance via Docker with the command:
```bash
docker run -d --publish=7474:7474 --publish=7687:7687 \
    --volume=`pwd`/neo4j:/data \
    -e NEO4J_dbms_memory_pagecache_size=2G \
    -e NEO4J_dbms_memory_heap_initial__size=4G \
    -e NEO4J_dbms_memory_heap_max__size=16G \
    neo4j
```

Note: You may wish to update the memory settings based on the amount of available memory on your machine.

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
CREATE INDEX ON :Relation(type, confidence)
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

### Missing Information
Find CITY_OF_HEADQUARTERS relation between two mentions where the linked entity doesn't have the relation we're looking for:
```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity)
OPTIONAL MATCH (e)-->(f:Fact {relation: r.type})
WHERE f IS NULL
RETURN d, s, r, o, e, f
LIMIT 25
```
