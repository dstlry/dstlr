# dstlr

`dstlr` is a system for large-scale knowledge extraction using [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), [Apache Spark](https://spark.apache.org/), and [neo4j](https://neo4j.com/). It takes a (potentially large) collection of unstructured text documents and horiztonally scales out CoreNLP via Spark to extract mentions of named entities, the relations between them, and links to an entity in an existing knowledge base. For relations of interest, we augment our extracted facts with corresponding facts from the knowledge base in order to reason about the quality of the text documents. From this, we generate a knowledge graph on which we can pose a number of queries via neo4j's Cypher query language to explore the text in a more structured manner.

We can discover a number of different scenarios relating facts asserted in documents to facts present in the knowledge base:
+ Supporting information - agreement between document and knowledge base
+ Inconsistent information - disagreement between document and knowledge base
+ Missing information - document contains information missing in knowledge base

Currently, we use Wikidata as a stand-in knowledge base and extract relations from nearly 600,000 Washington Post news articles. We extract 5,405,447 relations and 27,004,318 entity mentions (linking to 324,094 Wikidata entities).

## Supporting Information

```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE o.label = f.value
RETURN d, s, r, o, e, f
LIMIT 5
```

[https://en.wikipedia.org/wiki/International_Arctic_Research_Center](https://en.wikipedia.org/wiki/International_Arctic_Research_Center)

## Inconsistent Information

```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE NOT (o.label = f.value)
RETURN d, s, r, o, e, f
LIMIT 5
```

[https://en.wikipedia.org/wiki/International_Arctic_Research_Center](https://en.wikipedia.org/wiki/International_Arctic_Research_Center)

## Missing Information

```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity {id: "International_Arctic_Research_Center"})
OPTIONAL MATCH (e)-->(f:Fact {relation: r.type})
WHERE f IS NULL
RETURN d, s, r, o, e, f
LIMIT 25
```

[https://en.wikipedia.org/wiki/International_Arctic_Research_Center](https://en.wikipedia.org/wiki/International_Arctic_Research_Center)
