# dstlr

`dstlr` is a system for large-scale knowledge extraction using [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), [Apache Spark](https://spark.apache.org/), and [neo4j](https://neo4j.com/). It takes a (potentially large) collection of unstructured text documents and horiztonally scales out CoreNLP via Spark to extract mentions of named entities, the relations between them, and links to an entity in an existing knowledge base. For relations of interest, we augment our extracted facts with corresponding facts from the knowledge base in order to reason about the quality of the text documents. From this, we generate a knowledge graph on which we can pose a number of queries via neo4j's Cypher query language to explore the text in a more structured manner.

We can discover a number of different scenarios relating facts asserted in documents to facts present in the knowledge base:
+ Supporting information - agreement between document and knowledge base
+ Inconsistent information - disagreement between document and knowledge base
+ Missing information - document contains information missing in knowledge base

Currently, we use Wikidata as a stand-in knowledge base and extract relations from nearly 600,000 Washington Post news articles. We extract 5,405,447 relations and 27,004,318 entity mentions (linking to 324,094 Wikidata entities).

## Supporting Information

In this example, we see that the extractor correctly asserts that Good Technology has a headquarters in Sunnyvale..

```
MATCH (d:Document)-->(s:Mention {id: "b9428435-62cd-42f3-86a6-5631a3f5804c"})-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention {id: "0d5cfa92-70e5-42ef-ac1e-d9c2341c29d1"})
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE o.label = f.value
RETURN d, s, r, o, e, f
```

[https://en.wikipedia.org/wiki/Good_Technology](https://en.wikipedia.org/wiki/Good_Technology)

## Inconsistent Information

In this example, we see that the extractor incorrectly asserts that Isetan is located in Paris (when it is actually in Tokyo).

```
MATCH (d:Document)-->(s:Mention {id: "d1a6f6f6-864a-423d-b192-1a4ea08a42e1"})-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention {id: "5d6d99b3-ce3d-4f78-800f-6dc7270eeacf"})
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE NOT (o.label = f.value)
RETURN d, s, r, o, e, f
```

[https://en.wikipedia.org/wiki/Isetan](https://en.wikipedia.org/wiki/Isetan)

## Missing Information

In this example, the knowledge base has an entity for the "Internation Arctic Research Center", but doesn't have information regarding the headquarters.

```
MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention)
MATCH (s)-->(e:Entity {id: "International_Arctic_Research_Center"})
OPTIONAL MATCH (e)-->(f:Fact {relation: r.type})
WHERE f IS NULL
RETURN d, s, r, o, e, f
```

[https://en.wikipedia.org/wiki/International_Arctic_Research_Center](https://en.wikipedia.org/wiki/International_Arctic_Research_Center)
