package io.dstlr

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {

  val neoUri = opt[String](name = "neo4j.uri", default = Some("bolt://localhost:7687"))
  val neoUsername = opt[String](name = "neo4j.username", default = Some("neo4j"))
  val neoPassword = opt[String](name = "neo4j.password", default = Some("neo4j"))

  val contentField = opt[String](name = "content.field", default = Some("raw"))
  val searchField = opt[String](name = "search.field", default = Some("contents"))
  val searchTerm = opt[String](name = "search.term", default = Some("music"))

  verify()

}