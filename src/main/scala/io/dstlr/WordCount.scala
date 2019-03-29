package io.dstlr

import java.util.UUID

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.util.Pair
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MMap}

/**
  * Extract raw triples from documents on Solr using CoreNLP.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr - WordCount")
      .getOrCreate()

    import spark.implicits._

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Options for spark-solr
    val options = Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "fields" -> conf.fields(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Create a DataSet with the query results
    val ds = spark.read.format("solr")
      .options(options)
      .load()
      .as[SolrRow]

    ds.printSchema()

    val result = ds
      .repartition(conf.partitions())
      .filter(row => row.id != null && row.id.nonEmpty)
      .filter(row => row.contents != null && row.contents.nonEmpty)
      .map(row => (row.contents.split(" ").length, row.id))
      .toDF("length", "id")
      .sort($"length".desc)
      .coalesce(1)

    result.printSchema()

    // Write to parquet file
    result.write.csv(conf.output())

    spark.stop()

  }

  def buildMention(doc: String, entity: String, offsets: Pair[Integer, Integer]): TripleRow = {
    new TripleRow(doc, "Document", doc, "MENTIONS", "Entity", entity, Map("begin" -> offsets.first.toString, "end" -> offsets.second.toString))
  }

  def buildHasString(doc: String, entity: String, string: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "HAS_STRING", "Label", string, null)
  }

  def buildIs(doc: String, entity: String, entityType: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "IS_A", "EntityType", entityType, null)
  }

  def buildLinksTo(doc: String, entity: String, uri: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "LINKS_TO", "URI", uri, null)
  }

  def buildRelation(doc: String, uuids: MMap[String, UUID], triple: RelationTriple): TripleRow = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    new TripleRow(doc, "Entity", sub, rel, "Entity", obj, null)
  }
}