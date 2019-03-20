package io.dstlr

import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Statement}

import scala.collection.JavaConversions._

/**
  * Load the enriched triples into Neo4j
  */
object LoadTriples {

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    println(conf.summary)

    val spark = SparkSession
      .builder()
      .appName("dstlr - LoadTriples")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.option("header", "true").csv("triples").as[TripleRow]

    ds.foreachPartition(part => {
      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()
      part.foreach(row => {
        row.relation match {
          case "MENTIONS" => session.run(buildMention(row))
          case "HAS_STRING" => session.run(buildHasString(row))
          case "IS_A" => session.run(buildIs(row))
          case "LINKS_TO" => session.run(buildLinksTo(row))
          case _ => session.run(buildPredicate(row))
        }
      })
      session.close()
      db.close()
    })
  }

  def buildMention(row: TripleRow): Statement = {
    val params = Map("doc" -> row.doc, "entity" -> row.objectValue)
    new Statement("MERGE (d:Document {id: {doc}}) MERGE (e:Entity {id: {entity}}) MERGE (d)-[r:MENTIONS]->(e) RETURN d, r, e", params)
  }

  def buildHasString(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "string" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (l:Label {value: {string}}) MERGE (e)-[r:HAS_STRING]->(l) RETURN e, r, l", params)
  }

  def buildIs(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "entityType" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (t:EntityType {value: {entityType}}) MERGE (e)-[r:IS_A]->(t) RETURN e, r, t", params)
  }

  def buildLinksTo(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "uri" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (u:URI {id: {uri}}) MERGE (e)-[r:LINKS_TO]->(u) RETURN e, r, u", params)
  }

  def buildPredicate(row: TripleRow): Statement = {
    val params = Map(
      "doc" -> row.doc,
      "sub" -> row.subjectValue,
      "obj" -> row.objectValue
    )
    new Statement(
      s"""
         |MATCH (s:Entity {id:{sub}}),(o:Entity {id:{obj}})
         |MERGE (s)-[r:${row.relation}]->(o)
         |ON CREATE SET r.docs = [{doc}]
         |ON MATCH SET r.docs = r.docs + [{doc}]
       """.stripMargin, params)
  }

}