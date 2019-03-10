package io.dstlr

import org.rogach.scallop.{ScallopConf, Serialization}

class Conf(args: Seq[String]) extends ScallopConf(args) with Serialization {

  // Solr
  val solrUri = opt[String](name = "solr.uri", default = Some("localhost:9983"))
  val solrIndex = opt[String](name = "solr.index", default = Some("core17"))
  val query = opt[String](name = "query", default = Some("contents:firetruck"))
  val rows = opt[String](default = Some("10000"))
  val partitions = opt[Int](default = Some(16))

  // CoreNLP
  val nlpThreads = opt[String](default = Some("1"))

  // Neo4j
  val neoUri = opt[String](name = "neo4j.uri", default = Some("bolt://localhost:7687"))
  val neoUsername = opt[String](name = "neo4j.username", default = Some("neo4j"))
  val neoPassword = opt[String](name = "neo4j.password", default = Some("neo4j"))

  // Misc
  val output = opt[String](default = Some("triples"))

  verify()

}