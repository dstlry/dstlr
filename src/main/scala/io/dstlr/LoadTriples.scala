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

  // "HAS_STRING" trim length for neo4j
  val MAX_INDEX_SIZE = 1024

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    println(conf.summary)

    val spark = SparkSession
      .builder()
      .appName("dstlr - LoadTriples")
      .getOrCreate()

    import spark.implicits._

    val triples_acc = spark.sparkContext.longAccumulator("triples")

    val start = System.currentTimeMillis()

    // DataSet with a single partition for bulk loading
    val ds = spark.read.parquet(conf.input()).as[TripleRow]
      .coalesce(1)

    val notWikiDataValue = ds.filter($"objectType" =!= "WikiDataValue")

    // MENTIONS
    notWikiDataValue
      .filter($"relation" === "MENTIONS")
      .foreachPartition(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part.grouped(conf.neoBatchSize()).foreach(batch => {

          triples_acc.add(batch.size)

          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            val labelBytes = row.meta("label").getBytes("UTF-8")
            val label = if (labelBytes.length > MAX_INDEX_SIZE) {
              new String(labelBytes.slice(0, MAX_INDEX_SIZE))
            } else {
              row.meta("label")
            }
            list.append(new util.HashMap[String, String]() {
              {
                put("doc", row.doc)
                put("entity", row.objectValue)
                put("label", label)
                put("type", row.meta("type"))
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

    // LINKS_TO
    notWikiDataValue
      .filter($"relation" === "LINKS_TO" && $"objectValue".isNotNull)
      .foreachPartition(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
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
      .filter($"relation" =!= "LINKS_TO")
      .foreachPartition(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            list.append(new util.HashMap[String, String]() {
              {
                put("doc", row.doc)
                put("sub", row.subjectValue)
                put("rel", row.relation)
                put("obj", row.objectValue)
              }
            })
          })

          session.run(buildPredicate(list))

        })

        session.close()
        db.close()

      })

    // WikiDataValue
    ds
      .filter($"objectType" === "WikiDataValue")
      .foreachPartition(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            list.append(new util.HashMap[String, String]() {
              {
                put("uri", row.subjectValue)
                put("rel", row.relation)
                put("value", row.objectValue)
              }
            })
          })

          session.run(buildWikiData(list))

        })

        session.close()
        db.close()

      })

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${triples_acc.value / (duration / 1000)} triple/s")

    spark.stop()

  }

  def buildMention(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      """
        |UNWIND {batch} as batch
        |MERGE (d:Document {id: batch.doc})
        |MERGE (e:Entity {id: batch.entity, label: batch.label, type: batch.type})
        |MERGE (d)-[r:MENTIONS]->(e)
        |ON CREATE SET e.index = [batch.index]
        |ON MATCH SET e.index = e.index + [batch.index]
      """.stripMargin, params)
  }

  def buildLinksTo(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      """
        |UNWIND {batch} as batch
        |MATCH (e:Entity {id: batch.entity})
        |MERGE (u:URI {id: batch.uri})
        |MERGE (e)-[r:LINKS_TO]->(u)
      """.stripMargin, params)
  }

  def buildPredicate(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      s"""
         |UNWIND {batch} as batch
         |MATCH (s:Entity {id: batch.sub})
         |MATCH (o:Entity {id: batch.obj})
         |MERGE (s)-[:SUBJECT_OF]->(r:Relation {type: batch.rel})-[:OBJECT_OF]->(o)
       """.stripMargin, params)
  }

  def buildWikiData(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      s"""
         |UNWIND {batch} as batch
         |MATCH (u:URI {id: batch.uri})
         |MERGE (w:WikiDataValue {relation: batch.rel, value: batch.value})
         |MERGE (u)-[:HAS_VALUE]->(w)
      """.stripMargin, params)
  }
}