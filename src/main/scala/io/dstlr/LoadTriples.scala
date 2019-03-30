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
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.parquet(conf.input()).as[TripleRow].coalesce(1)

    ds.foreachPartition(part => {
      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()
      part.foreach(row => {

        if (row.objectType == "WikiDataValue") {
          session.run(buildWikiData(row))
        } else {
          row.relation match {
            case "MENTIONS" => session.run(buildMention(row))
            case "HAS_STRING" => session.run(buildHasString(row))
            case "IS_A" => session.run(buildIs(row))
            case "LINKS_TO" if row.objectValue != null => session.run(buildLinksTo(row))
            case _ => session.run(buildPredicate(row))
          }
        }
      })
      session.close()
      db.close()
    })
  }

  def buildMention(row: TripleRow): Statement = {
    val params = Map("doc" -> row.doc, "entity" -> row.objectValue, "index" -> s"${row.meta("begin")}-${row.meta("end")}")
    new Statement(
      """
        |MERGE (d:Document {id: {doc}})
        |MERGE (e:Entity {id: {entity}})
        |MERGE (d)-[r:MENTIONS]->(e)
        |ON CREATE SET r.index = [{index}]
        |ON MATCH SET r.index = r.index + [{index}]
      """.stripMargin, params)
  }

  def buildHasString(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "string" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (l:Label {value: {string}}) MERGE (e)-[r:HAS_STRING]->(l)", params)
  }

  def buildIs(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "entityType" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (t:EntityType {value: {entityType}}) MERGE (e)-[r:IS_A]->(t)", params)
  }

  def buildLinksTo(row: TripleRow): Statement = {
    val params = Map("entity" -> row.subjectValue, "uri" -> row.objectValue)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (u:URI {id: {uri}}) MERGE (e)-[r:LINKS_TO]->(u)", params)
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

  def buildWikiData(row: TripleRow): Statement = {
    val params = Map("uri" -> row.subjectValue, "value" -> row.objectValue)
    new Statement(s"MERGE (u:URI {id: {uri}}) MERGE (w:WikiDataValue {value: {value}}) MERGE (u)-[r:${row.relation}]->(w)", params)
  }
}