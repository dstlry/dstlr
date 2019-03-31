package io.dstlr

import java.util

import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Statement}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

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

    val ds = spark.read.parquet(conf.input()).as[TripleRow].coalesce(1)

    val notWikiDataValue = ds.filter($"objectType" =!= "WikiDataValue")

    // MENTIONS
    notWikiDataValue.filter($"relation" === "MENTIONS").foreachPartition(part => {

      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()

      part.grouped(conf.neoBatchSize()).foreach(batch => {

        val list = new util.ArrayList[util.Map[String, String]]()
        batch.foreach(row => {
          list.append(new util.HashMap[String, String]() {
            {
              put("doc", row.doc)
              put("entity", row.objectValue)
              put("index", s"${row.meta("begin")}-${row.meta("end")}")
            }
          })
        })

        // Insert the batch
        session.run(buildMention(list))

      })

      session.close()
      db.close()

    })

    // HAS_STRING
    notWikiDataValue.filter($"relation" === "HAS_STRING").foreachPartition(part => {

      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()

      part.grouped(conf.neoBatchSize()).foreach(batch => {

        val list = new util.ArrayList[util.Map[String, String]]()
        batch.foreach(row => {
          list.append(new util.HashMap[String, String]() {
            {
              put("entity", row.subjectValue)
              put("label", row.objectValue)
            }
          })
        })

        // Insert the batch
        session.run(buildHasString(list))

      })

      session.close()
      db.close()

    })

    // IS_A
    notWikiDataValue.filter($"relation" === "IS_A").foreachPartition(part => {

      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()

      part.grouped(conf.neoBatchSize()).foreach(batch => {

        val list = new util.ArrayList[util.Map[String, String]]()
        batch.foreach(row => {
          list.append(new util.HashMap[String, String]() {
            {
              put("entity", row.subjectValue)
              put("entityType", row.objectValue)
            }
          })
        })

        // Insert the batch
        session.run(buildIs(list))

      })

      session.close()
      db.close()

    })

    // LINKS_TO
    notWikiDataValue.filter($"relation" === "LINKS_TO" && $"objectValue".isNotNull).foreachPartition(part => {

      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()

      part.grouped(conf.neoBatchSize()).foreach(batch => {

        val list = new util.ArrayList[util.Map[String, String]]()
        batch.foreach(row => {
          list.append(new util.HashMap[String, String]() {
            {
              put("entity", row.subjectValue)
              put("uri", row.objectValue)
            }
          })
        })

        // Insert the batch
        session.run(buildLinksTo(list))

      })

      session.close()
      db.close()

    })

    notWikiDataValue
        .filter($"relation" =!= "MENTIONS")
        .filter($"relation" =!= "HAS_STRING")
        .filter($"relation" =!= "IS_A")
        .filter($"relation" =!= "LINKS_TO")
        .filter($"relation" =!= "MENTIONS")
        .foreachPartition(part => {
          val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
          val session = db.session()
          part.foreach(row => {
            session.run(buildPredicate(row))
          })
          session.close()
          db.close()
        })

    // WikiDataValue
    ds.filter($"objectType" === "WikiDataValue").foreachPartition(part => {
      val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
      val session = db.session()
      part.foreach(row => {
        session.run(buildWikiData(row))
      })
      session.close()
      db.close()
    })
  }

  // DONE
  def buildMention(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      """
        |UNWIND {batch} as batch
        |MERGE (d:Document {id: batch.doc})
        |MERGE (e:Entity {id: batch.entity})
        |MERGE (d)-[r:MENTIONS]->(e)
        |ON CREATE SET r.index = [batch.index]
        |ON MATCH SET r.index = r.index + [batch.index]
      """.stripMargin, params)
  }

  // DONE
  def buildHasString(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (l:Label {value: batch.label}) CREATE (e)-[r:HAS_STRING]->(l)", params)
  }

  // DONE
  def buildIs(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (t:EntityType {value: batch.entityType}) CREATE (e)-[r:IS_A]->(t)", params)
  }

  // DONE
  def buildLinksTo(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (u:URI {id: batch.uri}) CREATE (e)-[r:LINKS_TO]->(u)", params)
  }

  def buildPredicate(row: TripleRow): Statement = {
    val params = Map(
      "doc" -> row.doc,
      "sub" -> row.subjectValue,
      "obj" -> row.objectValue
    )
    new Statement(
      s"""
         |MATCH (s:Entity {id:{sub}})
         |MATCH (o:Entity {id:{obj}})
         |MERGE (s)-[r:${row.relation}]->(o)
         |ON CREATE SET r.docs = [{doc}]
         |ON MATCH SET r.docs = r.docs + [{doc}]
       """.stripMargin, params)
  }

  def buildWikiData(row: TripleRow): Statement = {
    val params = Map("uri" -> row.subjectValue, "value" -> row.objectValue)
    new Statement(s"MATCH (u:URI {id: {uri}}) MERGE (w:WikiDataValue {value: {value}}) CREATE (u)-[r:${row.relation}]->(w)", params)
  }
}