package io

import org.rogach.scallop.{ScallopConf, Serialization}

package object dstlr {

  // Row for spark-solr results based on Anserini's schema
  case class SolrRow(id: String, contents: String)

  // Row for the CoreNLP extractions
  case class TripleRow(doc: String, subjectType: String, subjectValue: String, relation: String, objectType: String, objectValue: String, meta: Map[String, String])

  // Row for mapping from WikiData property ID to relation name
  case class WikiDataMappingRow(property: String, relation: String)

  class Conf(args: Seq[String]) extends ScallopConf(args) with Serialization {

    // I/O
    val input = opt[String](default = Some("triples"))
    val output = opt[String](default = Some("triples"))

    // Solr
    val solrUri = opt[String](name = "solr.uri", default = Some("localhost:9983"))
    val solrIndex = opt[String](name = "solr.index", default = Some("core18"))
    val query = opt[String](name = "query", default = Some("*:*"))
    val fields = opt[String](name = "fields", default = Some("id,contents"))
    val rows = opt[String](default = Some("10000"))
    val partitions = opt[Int](default = Some(8))
    val docLengthThreshold = opt[Int](default = Some(10000))
    val sentLengthThreshold = opt[Int](default = Some(256))

    // Neo4j
    val neoUri = opt[String](name = "neo4j.uri", default = Some("bolt://localhost:7687"))
    val neoUsername = opt[String](name = "neo4j.username", default = Some("neo4j"))
    val neoPassword = opt[String](name = "neo4j.password", default = Some("neo4j"))
    val neoBatchSize = opt[Int](name = "neo4j.batch.size", default = Some(10000))

    verify()

  }
}