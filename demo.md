# dstlr

`dstlr` is a system for large-scale knowledge extraction using [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/), [Apache Spark](https://spark.apache.org/), and [neo4j](https://neo4j.com/). It takes a (potentially large) collection of unstructured text documents and horiztonally scales out CoreNLP via Spark to extract mentions of named entities, the relations between them, and links to an entity in an existing knowledge base. For relations of interest, we augment our extracted facts with corresponding facts from the knowledge base in order to reason about the quality of the text documents. From this, we generate a knowledge graph on which we can pose a number of queries via neo4j's Cypher query language to explore the text in a more structured manner.

We can discover a number of different scenarios relating facts asserted in documents to facts present in the knowledge base:
+ Supporting information - agreement between document and knowledge base
+ Inconsistent information - disagreement between document and knowledge base
+ Missing information - document contains information missing in knowledge base

Currently, we use Wikidata as a stand-in knowledge base and extract relations from nearly 600,000 Washington Post news articles. We extract 5,405,447 relations and 27,004,318 entity mentions (linking to 324,094 Wikidata entities).

## Supporting Information

```
MATCH (d:Document)-->(s:Mention {id: "3b5363f7-ad90-4a8e-86e3-0fa1442d9bdc"})-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention {id: "e6766c8e-f1dc-420a-831b-9d6926e7a230"})
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE o.label = f.value
RETURN d, s, r, o, e, f
LIMIT 5
```

[https://en.wikipedia.org/wiki/University_of_Maryland,_College_Park](https://en.wikipedia.org/wiki/University_of_Maryland,_College_Park)

## Inconsistent Information

```
MATCH (d:Document)-->(s:Mention {id: "52807bb9-265a-45ae-8d2b-95707e43033d"})-->(r:Relation {type: "CITY_OF_HEADQUARTERS"})-->(o:Mention {id: "590b9b76-22c6-4287-a75a-10f6201e5b77"})
MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type})
WHERE NOT (o.label = f.value)
RETURN d, s, r, o, e, f
LIMIT 25
```

[https://en.wikipedia.org/wiki/Harvard_University](https://en.wikipedia.org/wiki/Harvard_University)

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
