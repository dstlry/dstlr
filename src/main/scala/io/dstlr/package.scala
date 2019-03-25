package io

import org.rogach.scallop.{ScallopConf, Serialization}

package object dstlr {

  // Result from spark-solr
  case class SolrRow(id: String, contents: String)

  // Result of our extraction
  case class TripleRow(doc: String, subjectType: String, subjectValue: String, relation: String, objectType: String, objectValue: String, meta: Map[String, String])

  // Mapping from WikiData properties to our relation names
  case class WikiDataMappingRow(property: String, relation: String)

  class Conf(args: Seq[String]) extends ScallopConf(args) with Serialization {

    // Solr
    val solrUri = opt[String](name = "solr.uri", default = Some("localhost:9983"))
    val solrIndex = opt[String](name = "solr.index", default = Some("core18"))
    val query = opt[String](name = "query", default = Some("*:*"))
    val rows = opt[String](default = Some("10000"))
    val partitions = opt[Int](default = Some(8))

    // CoreNLP
    val nlpThreads = opt[String](default = Some("8"))

    // Neo4j
    val neoUri = opt[String](name = "neo4j.uri", default = Some("bolt://localhost:7687"))
    val neoUsername = opt[String](name = "neo4j.username", default = Some("neo4j"))
    val neoPassword = opt[String](name = "neo4j.password", default = Some("neo4j"))

    // Misc
    val input = opt[String](default = Some("triples"))
    val output = opt[String](default = Some("triples"))

    verify()

  }

}