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

    val notWikiDataValue = ds.filter($"objectType" =!= "Fact")

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
            val spanBytes = row.meta("span").getBytes("UTF-8")
            val span = if (spanBytes.length > MAX_INDEX_SIZE) {
              new String(spanBytes.slice(0, MAX_INDEX_SIZE))
            } else {
              row.meta("span")
            }
            list.append(new util.HashMap[String, String]() {
              {
                put("doc", row.doc)
                put("mention", row.objectValue)
                put("class", row.meta("class"))
                put("span", span)
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
                put("mention", row.subjectValue)
                put("entity", row.objectValue)
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
                put("subject", row.subjectValue)
                put("relation", row.relation)
                put("object", row.objectValue)
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
      .filter($"objectType" === "Fact")
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
                put("relation", row.relation)
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
        |MERGE (m:Mention {id: batch.mention, class: batch.class, span: batch.span})
        |MERGE (d)-[r:MENTIONS]->(m)
        |ON CREATE SET m.index = [batch.index]
        |ON MATCH SET m.index = m.index + [batch.index]
      """.stripMargin, params)
  }

  def buildLinksTo(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      """
        |UNWIND {batch} as batch
        |MATCH (m:Mention {id: batch.mention})
        |MERGE (e:Entity {id: batch.entity})
        |MERGE (m)-[r:LINKS_TO]->(e)
      """.stripMargin, params)
  }

  def buildPredicate(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      s"""
         |UNWIND {batch} as batch
         |MATCH (s:Mention {id: batch.subject})
         |MATCH (o:Mention {id: batch.object})
         |MERGE (s)-[:SUBJECT_OF]->(r:Relation {type: batch.relation})-[:OBJECT_OF]->(o)
       """.stripMargin, params)
  }

  def buildWikiData(batch: util.ArrayList[util.Map[String, String]]): Statement = {
    val params = Map("batch" -> batch)
    new Statement(
      s"""
         |UNWIND {batch} as batch
         |MATCH (e:Entity {id: batch.entity})
         |MERGE (f:Fact {relation: batch.relation, value: batch.value})
         |MERGE (e)-[:HAS_FACT]->(f)
      """.stripMargin, params)
  }
}