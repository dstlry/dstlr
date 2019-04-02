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

  // Max size of indexes in neo4j
  val MAX_INDEX_SIZE = 4039

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

    val ds = spark.read.parquet(conf.input()).as[TripleRow].coalesce(1)

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
    notWikiDataValue
      .filter($"relation" === "HAS_STRING")
      .foreachPartition(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            val labelBytes = row.objectValue.getBytes("UTF-8")
            list.append(new util.HashMap[String, String]() {
              {
                put("entity", row.subjectValue)
                put("label", if (labelBytes.length < MAX_INDEX_SIZE) row.objectValue else new String(labelBytes.slice(0, MAX_INDEX_SIZE - 1)))
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
    notWikiDataValue
      .filter($"relation" === "IS_A")
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
      .filter($"relation" =!= "HAS_STRING")
      .filter($"relation" =!= "IS_A")
      .filter($"relation" =!= "LINKS_TO")
      .filter($"relation" =!= "MENTIONS")
      .rdd
      .groupBy(row => row.relation)
      .foreach(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part._2.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            list.append(new util.HashMap[String, String]() {
              {
                put("doc", row.doc)
                put("sub", row.subjectValue)
                put("obj", row.objectValue)
              }
            })
          })

          session.run(buildPredicate(part._1, list))

        })

        session.close()
        db.close()

      })

    // WikiDataValue
    ds
      .filter($"objectType" === "WikiDataValue")
      .rdd
      .groupBy(row => row.relation)
      .foreach(part => {

        val db = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
        val session = db.session()

        part._2.grouped(conf.neoBatchSize()).foreach(batch => {
          triples_acc.add(batch.size)
          val list = new util.ArrayList[util.Map[String, String]]()
          batch.foreach(row => {
            list.append(new util.HashMap[String, String]() {
              {
                put("uri", row.subjectValue)
                put("value", row.objectValue)
              }
            })
          })

          session.run(buildWikiData(part._1, list))

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
        |MERGE (e:Entity {id: batch.entity})
        |MERGE (d)-[r:MENTIONS]->(e)
        |ON CREATE SET r.index = [batch.index]
        |ON MATCH SET r.index = r.index + [batch.index]
      """.stripMargin, params)
  }

  def buildHasString(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (l:Label {value: batch.label}) MERGE (e)-[r:HAS_STRING]->(l)", params)
  }

  def buildIs(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (t:EntityType {value: batch.entityType}) MERGE (e)-[r:IS_A]->(t)", params)
  }

  def buildLinksTo(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement("UNWIND {batch} as batch MATCH (e:Entity {id: batch.entity}) MERGE (u:URI {id: batch.uri}) MERGE (e)-[r:LINKS_TO]->(u)", params)
  }

  def buildPredicate(relation: String, batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      s"""
         |UNWIND {batch} as batch
         |MATCH (s:Entity {id: batch.sub})
         |MATCH (o:Entity {id: batch.obj})
         |MERGE (s)-[r:${relation}]->(o)
         |ON CREATE SET r.docs = [batch.doc]
         |ON MATCH SET r.docs = r.docs + [batch.doc]
       """.stripMargin, params)
  }

  def buildWikiData(relation: String, batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(s"UNWIND {batch} as batch MATCH (u:URI {id: batch.uri}) MERGE (w:WikiDataValue {value: batch.value}) MERGE (u)-[r:${relation}]->(w)", params)
  }
}