# dstlr

`dstlr` is an open-source platform for scalable, end-to-end knowledge graph construction from unstructured text. The platform takes a collection of documents, extracts mentions and relations to populate a raw knowledge graph, links mentions to entities in Wikidata, and then enriches the knowledge graph with facts from Wikidata.
See [`dstlr.ai`](http://dstlr.ai/) for an overview of the platform.

The current `dstlr` demo "distills" the [TREC Washington Post Corpus](https://trec.nist.gov/data/wapost/) containing around 600K documents into a raw knowledge graph comprised of approximately 97M triples, enriched with facts from Wikidata for the 324K distinct entities discovered in the corpus.
On top of thos knowledge graph, we have implemented a subgraph-matching approach to align extracted relations with facts from Wikidata using the declarative Cypher query language.
This simple demo shows that fact verification, locating textual support for asserted facts, detecting inconsistent and missing facts, and extracting distantly-supervised training data can all be performed within the same framework.
 
This readme provies instructions on how to replicate our work.

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
